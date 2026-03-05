"""
fast_proxy - 通用智能下载代理
支持 Docker/pip/R 包的多线程加速下载 + 缓存
"""
import os
import sys
import yaml
import logging
import asyncio
import hashlib
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import StreamingResponse

from router import Router
from cache import CacheManager
from downloader import ParallelDownloader

# 配置日志
def setup_logging(level: str, logfile: str):
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(logfile),
            logging.StreamHandler(sys.stdout)
        ]
    )

# 全局变量
config: dict = {}
router: Router = None
cache: CacheManager = None
http_client: httpx.AsyncClient = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    global config, router, cache, http_client
    
    # 加载配置
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    setup_logging(
        config.get('logging', {}).get('level', 'INFO'),
        config.get('logging', {}).get('file', '/tmp/fast_proxy.log')
    )
    
    # 初始化组件
    router = Router(config['rules'])
    cache = CacheManager(
        config['cache']['dir'],
        config['cache']['max_size_gb']
    )
    
    # HTTP 客户端（带代理支持）
    proxy = config['server'].get('upstream_proxy')
    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(300.0),
        follow_redirects=True,
        proxy=proxy
    )
    
    logging.info(f"fast_proxy started, upstream_proxy={proxy}")
    yield
    
    # 清理
    await http_client.aclose()
    logging.info("fast_proxy stopped")

app = FastAPI(lifespan=lifespan)

async def _proxy_request(request: Request, target_url: str) -> Response:
    """直接代理转发请求"""
    headers = {k: v for k, v in request.headers.items() 
               if k.lower() not in ['host', 'connection', 'content-length']}
    
    async with http_client.stream(
        request.method, target_url, headers=headers,
        content=await request.body(),
        follow_redirects=True
    ) as resp:
        content = await resp.aread()
        # 过滤掉可能导致问题的响应头
        response_headers = {k: v for k, v in resp.headers.items() 
                           if k.lower() not in ['content-length', 'transfer-encoding', 'content-encoding']}
        return Response(
            content=content,
            status_code=resp.status_code,
            headers=response_headers
        )

async def _file_iterator(filepath: str, chunk_size: int = 8192):
    """文件流式迭代器"""
    with open(filepath, 'rb') as f:
        while chunk := f.read(chunk_size):
            yield chunk

async def _parallel_download(request: Request, target_url: str, rule) -> Response:
    """并行下载 + 缓存策略"""
    logging.info(f"Entering _parallel_download for: {target_url}")
    # 1. 先 HEAD 获取文件大小和内容类型，跟随重定向
    # 带上客户端的 Authorization header（用于 Docker Registry 认证）
    headers = {}
    auth_header = request.headers.get('authorization')
    if auth_header:
        headers['Authorization'] = auth_header
    
    head_resp = await http_client.head(target_url, headers=headers, follow_redirects=True)
    # 获取最终 URL（如果有重定向）
    final_url = str(head_resp.url)
    logging.info(f"Final URL after redirect: {final_url}")
    content_length = int(head_resp.headers.get('content-length', 0))
    content_type = head_resp.headers.get('content-type', '')
    logging.info(f"HEAD response: content_length={content_length}, content_type={content_type}")
    
    # 检查文件大小是否符合规则
    if content_length < rule.min_size:
        logging.info(f"File size {content_length} < min_size {rule.min_size}, fallback to proxy")
        return await _proxy_request(request, target_url)
    
    # 2. 检查缓存
    # 根据规则配置决定使用哪个 URL 作为缓存 key
    # target_url 是原始请求 URL，final_url 是重定向后的 URL（可能包含临时签名）
    cache_key_source = getattr(rule, 'cache_key_source', 'final')
    if cache_key_source == 'original':
        cache_key = target_url
        logging.info(f"Using original URL as cache key: {cache_key}")
    else:
        cache_key = final_url
    
    cached = cache.get(cache_key, content_type)
    if cached:
        logging.info(f"Cache HIT: {cache_key}")
        return StreamingResponse(
            _file_iterator(cached),
            media_type=content_type,
            headers={"Content-Length": str(os.path.getsize(cached))}
        )
    
    # 3. 并行下载（使用最终 URL）
    logging.info(f"Cache MISS, parallel download: {final_url} (size: {content_length} bytes)")
    # 使用 URL 的 hash 作为文件名，避免 URL 参数导致文件名过长
    url_hash = hashlib.sha256(cache_key.encode()).hexdigest()[:16]
    temp_file = os.path.join(config['cache']['dir'], f"tmp_{url_hash}")
    
    # 如果有 Authorization header，需要传递给下载器
    downloader = ParallelDownloader(
        url=final_url,
        filepath=temp_file,
        concurrency=rule.concurrency,
        chunk_size=rule.chunk_size,
        proxy=config['server'].get('upstream_proxy'),
        headers=headers if auth_header else None
    )
    
    try:
        filepath = await downloader.download()
    except Exception as e:
        logging.error(f"Download failed: {e}")
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise HTTPException(502, f"Download error: {str(e)}")
    
    # 4. 存入缓存（使用 cache_key）
    cache.put(cache_key, filepath, content_type)
    
    # 5. 流式返回内容
    return StreamingResponse(
        _file_iterator(filepath),
        media_type=content_type,
        headers={"Content-Length": str(os.path.getsize(filepath))}
    )

@app.api_route("/{full_path:path}", methods=["GET", "HEAD", "POST", "PUT", "DELETE"])
async def proxy_handler(request: Request, full_path: str):
    """统一代理入口"""
    logging.info(f"Received request: {request.method} {full_path}")
    # 构建目标 URL (默认 PyPI，可扩展)
    # 支持多 upstream，这里简化为 PyPI + Docker Hub
    if full_path.startswith("v2/"):
        base = "https://registry-1.docker.io"
        target_url = f"{base}/{full_path}"
    elif full_path.startswith("packages/") or full_path.startswith("simple/"):
        base = "https://pypi.org"
        target_url = f"{base}/{full_path}"
    elif full_path.startswith("src/contrib/") and full_path.endswith(".tar.gz"):
        # R 包路径，路由到 CRAN
        base = "https://cran.r-project.org"
        target_url = f"{base}/{full_path}"
    elif "/blob/main/" in full_path or "/resolve/main/" in full_path:
        # HuggingFace 模型文件
        # 将 /blob/main/ 转换为 /resolve/main/ 用于实际下载
        base = "https://huggingface.co"
        resolved_path = full_path.replace("/blob/main/", "/resolve/main/")
        target_url = f"{base}/{resolved_path}"
    else:
        # 默认 PyPI
        base = "https://pypi.org"
        target_url = f"{base}/{full_path}"
    
    logging.info(f"Target URL: {target_url}")
    
    # 路由匹配（使用 full_path 而不是 request.url.path）
    # 先按路径匹配规则，不检查大小（大小检查在 _parallel_download 中进行）
    rule = router.match('/' + full_path, content_length=None)
    logging.info(f"Matched rule: {rule.name if rule else None}, strategy: {rule.strategy if rule else None}")
    
    if not rule or rule.strategy == 'proxy':
        logging.info("Using proxy strategy")
        return await _proxy_request(request, target_url)
    
    if rule.strategy == 'parallel':
        # 仅 GET/HEAD 支持并行下载
        if request.method not in ['GET', 'HEAD']:
            logging.info(f"Method {request.method} not supported for parallel, using proxy")
            return await _proxy_request(request, target_url)
        logging.info("Using parallel download strategy")
        return await _parallel_download(request, target_url, rule)
    
    # fallback
    return await _proxy_request(request, target_url)

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/stats")
async def stats():
    return {"cache": cache.get_stats()}

if __name__ == "__main__":
    import uvicorn
    # 加载配置
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    uvicorn.run(
        "main:app",
        host=config['server']['host'],
        port=config['server']['port'],
        log_level="info"
    )

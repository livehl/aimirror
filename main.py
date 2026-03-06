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

from router import Router, Rule
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
    # 如果 proxy 为空字符串，设为 None
    if not proxy:
        proxy = None
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

async def _proxy_request(request: Request, target_url: str, rule: Rule = None) -> Response:
    """直接代理转发请求，支持内容改写"""
    headers = {k: v for k, v in request.headers.items() 
               if k.lower() not in ['host', 'connection', 'content-length']}
    
    async with http_client.stream(
        request.method, target_url, headers=headers,
        content=await request.body(),
        follow_redirects=True
    ) as resp:
        content = await resp.aread()
        content_type = resp.headers.get('content-type', '')
        
        # 根据 Content-Type 改写内容中的 URL
        if rule:
            content = _rewrite_html_urls(content, rule, content_type)
        
        # 过滤掉可能导致问题的响应头
        response_headers = {k: v for k, v in resp.headers.items() 
                           if k.lower() not in ['content-length', 'transfer-encoding', 'content-encoding']}
        return Response(
            content=content,
            status_code=resp.status_code,
            headers=response_headers
        )

def _rewrite_html_urls(content: bytes, rule: Rule, content_type: str) -> bytes:
    """改写响应内容中的上游 URL 为代理 URL
    
    Args:
        content: 响应内容
        rule: 匹配的规则
        content_type: 响应的 Content-Type
    """
    # 如果没有配置 content_rewrite，不进行改写
    if not rule.content_rewrite:
        return content
    
    # 检查 Content-Type 是否匹配
    content_types = rule.content_rewrite.get('content_types', [])
    if not any(ct in content_type for ct in content_types):
        return content
    
    # 获取要替换的目标 host 列表
    targets = rule.content_rewrite.get('targets', [])
    if not targets:
        return content
    
    try:
        text = content.decode('utf-8')
        
        # 从配置中获取对外访问地址
        proxy_host = config['server'].get('public_host', f"127.0.0.1:{config['server']['port']}")
        proxy_base = f"http://{proxy_host}"
        
        # 替换所有目标 host 为代理地址
        for target in targets:
            text = text.replace(target, proxy_base)
        
        return text.encode('utf-8')
    except Exception as e:
        logging.warning(f"Failed to rewrite content URLs: {e}")
        return content

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
        return await _proxy_request(request, target_url, rule)
    
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

@app.get("/health")
async def health():
    """健康检查端点"""
    return {"status": "ok"}

@app.get("/stats")
async def stats():
    """缓存统计端点"""
    return {"cache": cache.get_stats()}

@app.api_route("/{full_path:path}", methods=["GET", "HEAD", "POST", "PUT", "DELETE"])
async def proxy_handler(request: Request, full_path: str):
    """统一代理入口"""
    logging.info(f"Received request: {request.method} {full_path}")
    
    # 路由匹配（使用 full_path 而不是 request.url.path）
    rule = router.match('/' + full_path, content_length=None)
    logging.info(f"Matched rule: {rule.name if rule else None}, strategy: {rule.strategy if rule else None}")
    
    if not rule:
        raise HTTPException(404, "No matching rule found")
    
    # 构建目标 URL
    target_url = rule.build_target_url('/' + full_path)
    
    logging.info(f"Target URL: {target_url}")
    
    if rule.strategy == 'proxy':
        logging.info("Using proxy strategy")
        return await _proxy_request(request, target_url, rule)
    
    if rule.strategy == 'parallel':
        # 仅 GET/HEAD 支持并行下载
        if request.method not in ['GET', 'HEAD']:
            logging.info(f"Method {request.method} not supported for parallel, using proxy")
            return await _proxy_request(request, target_url, rule)
        logging.info("Using parallel download strategy")
        return await _parallel_download(request, target_url, rule)
    
    # fallback
    return await _proxy_request(request, target_url, rule)

def main():
    """CLI入口点"""
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

if __name__ == "__main__":
    main()

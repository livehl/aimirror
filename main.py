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
download_semaphore: asyncio.Semaphore = None  # 全局下载并发控制
active_downloads: dict = {}  # 正在下载的文件 {cache_key: asyncio.Event}

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    global config, router, cache, http_client, download_semaphore
    
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
    
    # 初始化全局下载并发控制信号量
    max_concurrent_downloads = config.get('server', {}).get('max_concurrent_downloads', 10)
    download_semaphore = asyncio.Semaphore(max_concurrent_downloads)
    logging.info(f"Global download concurrency limit: {max_concurrent_downloads}")
    
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
            content = _rewrite_content_urls(content, rule, content_type)
        
        # 过滤掉可能导致问题的响应头
        response_headers = {k: v for k, v in resp.headers.items() 
                           if k.lower() not in ['content-length', 'transfer-encoding', 'content-encoding']}
        return Response(
            content=content,
            status_code=resp.status_code,
            headers=response_headers
        )

def _rewrite_content_urls(content: bytes, rule: Rule, content_type: str) -> bytes:
    """改写响应内容中的上游 URL 为代理 URL
    
    Args:
        content: 响应内容
        rule: 匹配的规则
        content_type: 响应的 Content-Type
    """
    # 如果没有配置 content_rewrite，不进行改写
    if not rule.content_rewrite:
        return content
    
    # 检查 Content-Type 是否匹配（支持 HTML 和 JSON）
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

async def _streaming_file_iterator(filepath: str, download_task: asyncio.Task, total_size: int, chunk_size: int = 65536):
    """流式文件迭代器 - 边下载边返回数据，确保数据已下载才返回"""
    last_size = 0
    max_wait = 600  # 最多等待 600 秒
    start_time = asyncio.get_event_loop().time()
    
    with open(filepath, 'rb') as f:
        while last_size < total_size:
            # 检查是否超时
            if asyncio.get_event_loop().time() - start_time > max_wait:
                raise TimeoutError(f"Download timeout after {max_wait}s")
            
            # 获取当前文件大小
            f.seek(0, 2)
            current_size = f.tell()
            
            # 如果当前位置的数据还没准备好，等待
            if current_size <= last_size:
                # 检查下载任务是否失败
                if download_task.done():
                    exc = download_task.exception()
                    if exc:
                        raise exc
                    # 任务完成但文件大小不够，可能出错了
                    if current_size < total_size:
                        raise RuntimeError("Download incomplete")
                    break
                
                # 数据还没准备好，等待一小段时间
                await asyncio.sleep(0.05)
                continue
            
            # 读取已准备好的数据
            f.seek(last_size)
            bytes_to_read = min(current_size - last_size, chunk_size)
            chunk = f.read(bytes_to_read)
            
            if chunk:
                yield chunk
                last_size += len(chunk)
        
        # 下载完成，确保读取所有剩余数据
        f.seek(last_size)
        while chunk := f.read(chunk_size):
            yield chunk

async def _parallel_download(request: Request, target_url: str, rule) -> Response:
    """并行下载 + 缓存策略（带全局并发控制和流式返回）"""
    logging.info(f"Entering _parallel_download for: {target_url}")
    
    # 1. 先 HEAD 获取文件大小和内容类型，跟随重定向
    headers = {}
    auth_header = request.headers.get('authorization')
    if auth_header:
        headers['Authorization'] = auth_header
    
    head_resp = await http_client.head(target_url, headers=headers, follow_redirects=True)
    final_url = str(head_resp.url)
    content_length = int(head_resp.headers.get('content-length', 0))
    content_type = head_resp.headers.get('content-type', '')
    
    # 检查文件大小是否符合规则
    if content_length < rule.min_size:
        return await _proxy_request(request, target_url, rule)
    
    # 2. 确定缓存 key
    cache_key_source = getattr(rule, 'cache_key_source', 'final')
    cache_key = target_url if cache_key_source == 'original' else final_url
    
    # 3. 检查缓存
    cached = cache.get(cache_key, content_type)
    if cached:
        return StreamingResponse(
            _file_iterator(cached),
            media_type=content_type,
            headers={"Content-Length": str(os.path.getsize(cached))}
        )
    
    # 4. 检查是否正在下载中
    if cache_key in active_downloads:
        temp_file, download_task, start_time = active_downloads[cache_key]
        
        # 检查下载是否已完成
        if download_task.done():
            exc = download_task.exception()
            if exc:
                raise HTTPException(502, f"Download failed: {str(exc)}")
            # 下载完成，等待缓存写入完成
            for _ in range(10):  # 最多等待 5 秒
                cached = cache.get(cache_key, content_type)
                if cached:
                    return StreamingResponse(
                        _file_iterator(cached),
                        media_type=content_type,
                        headers={"Content-Length": str(os.path.getsize(cached))}
                    )
                await asyncio.sleep(0.5)
            # 缓存还没准备好，返回 302 让客户端再试一次
            logging.warning(f"Download done but cache not ready: {cache_key}")
        
        # 还在下载中，返回 302 让客户端稍后重试
        elapsed = asyncio.get_event_loop().time() - start_time
        logging.info(f"File downloading, returning 302: {cache_key} (elapsed: {elapsed:.1f}s)")
        return Response(
            status_code=302,
            headers={
                "Location": str(request.url),
                "Retry-After": "3",
                "Cache-Control": "no-cache, no-store, must-revalidate"
            }
        )
    
    # 5. 使用全局信号量控制并发，并开始下载
    async with download_semaphore:
        # 再次检查缓存（可能其他线程已完成）
        cached = cache.get(cache_key, content_type)
        if cached:
            return StreamingResponse(
                _file_iterator(cached),
                media_type=content_type,
                headers={"Content-Length": str(os.path.getsize(cached))}
            )
        
        # 准备下载
        url_hash = hashlib.sha256(cache_key.encode()).hexdigest()[:16]
        temp_file = os.path.join(config['cache']['dir'], f"tmp_{url_hash}")
        
        try:
            # 创建下载器，启用流式模式
            downloader = ParallelDownloader(
                url=final_url,
                filepath=temp_file,
                concurrency=rule.concurrency,
                chunk_size=rule.chunk_size,
                proxy=config['server'].get('upstream_proxy'),
                headers=headers if auth_header else None,
                stream_mode=True  # 启用流式模式
            )
            
            # 启动后台下载任务
            download_task = asyncio.create_task(
                downloader.download_with_streaming(cache_key, temp_file, cache, content_type)
            )
            
            # 标记为正在下载（存储文件路径、任务和开始时间）
            start_time = asyncio.get_event_loop().time()
            active_downloads[cache_key] = (temp_file, download_task, start_time)
            
            # 每秒检测一次，下载完成立即返回，最多等待15秒
            try:
                for _ in range(15):
                    await asyncio.sleep(1)
                    
                    # 检查下载是否完成
                    if download_task.done():
                        exc = download_task.exception()
                        if exc:
                            raise HTTPException(502, f"Download failed: {str(exc)}")
                        # 等待缓存写入
                        for _ in range(10):
                            cached = cache.get(cache_key, content_type)
                            if cached:
                                return StreamingResponse(
                                    _file_iterator(cached),
                                    media_type=content_type,
                                    headers={"Content-Length": str(os.path.getsize(cached))}
                                )
                            await asyncio.sleep(0.5)
                        break  # 缓存还没好，返回302
                
                logging.info(f"Download not ready, returning 302: {cache_key}")
                return Response(
                    status_code=302,
                    headers={
                        "Location": str(request.url),
                        "Retry-After": "3",
                        "Cache-Control": "no-cache, no-store, must-revalidate"
                    }
                )
            except asyncio.CancelledError:
                # 客户端断开连接，让后台下载继续运行
                logging.info(f"Client disconnected, download continuing in background: {cache_key}")
                raise  # 重新抛出，让 FastAPI 处理断开连接
            except Exception as e:
                logging.error(f"Download failed: {e}")
                # 清理状态
                if cache_key in active_downloads:
                    del active_downloads[cache_key]
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                # 取消后台下载任务
                if not download_task.done():
                    download_task.cancel()
                raise HTTPException(502, f"Download error: {str(e)}")
        
        except Exception as e:
            logging.error(f"Download setup failed: {e}")
            raise HTTPException(502, f"Download setup error: {str(e)}")

@app.get("/health")
async def health():
    """健康检查端点"""
    return {
        "status": "ok",
        "active_downloads": len(active_downloads),
        "downloads": list(active_downloads.keys())
    }

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

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
import importlib
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import StreamingResponse

from router import Router, Rule
from cache import CacheManager
from downloader import ParallelDownloader


def load_handler(handler_path: str):
    """动态加载 handler 模块
    
    Args:
        handler_path: 模块路径，如 "handlers.docker"
    
    Returns:
        模块对象，如果加载失败返回 None
    """
    try:
        module = importlib.import_module(handler_path)
        if not hasattr(module, 'exec_path'):
            logging.error(f"Handler {handler_path} does not have exec_path function")
            return None
        return module
    except ImportError as e:
        logging.error(f"Failed to load handler {handler_path}: {e}")
        return None

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
sequential_download_semaphore: asyncio.Semaphore = None  # chunk_size=0 时的串行下载控制（只允许一个任务）
active_downloads: dict = {}  # 正在下载的文件 {cache_key: asyncio.Event}
upstream_proxy: Optional[str] = None  # 上游代理配置

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    global config, router, cache, http_client, download_semaphore, sequential_download_semaphore
    
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
    
    # 初始化 chunk_size=0 时的串行下载信号量（只允许一个任务并行）
    sequential_download_semaphore = asyncio.Semaphore(1)
    logging.info("Sequential download mode (chunk_size=0) limit: 1")
    
    # HTTP 客户端（带代理支持）
    global upstream_proxy
    upstream_proxy = config['server'].get('upstream_proxy')
    # 如果 proxy 为空字符串，设为 None
    if not upstream_proxy:
        upstream_proxy = None
    
    # 配置连接池限制，避免 PoolTimeout
    limits = httpx.Limits(
        max_connections=200,           # 最大连接数
        max_keepalive_connections=50,  # 最大保持连接数
        keepalive_expiry=30.0          # 保持连接过期时间(秒)
    )
    
    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(300.0, pool=5),  # pool=5秒连接池获取超时
        follow_redirects=True,
        proxy=upstream_proxy,
        limits=limits
    )
    
    logging.info(f"fast_proxy started, upstream_proxy={upstream_proxy}")
    yield
    
    # 清理
    await http_client.aclose()
    logging.info("fast_proxy stopped")

app = FastAPI(lifespan=lifespan)

async def _proxy_request(request: Request, target_url: str, rule: Rule = None, processed_path: str = None) -> Response:
    """直接代理转发请求，支持内容改写"""
    headers = {k: v for k, v in request.headers.items()
               if k.lower() not in ['host', 'connection', 'content-length']}

    # HEAD 请求且配置了 head_meta_headers 时，预获取元数据头部
    meta_headers = {}
    if request.method == "HEAD" and rule and rule.head_meta_headers:
        # 先获取元数据（不跟随重定向）
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0), proxy=upstream_proxy) as head_client:
            meta_resp = await head_client.head(target_url, headers=headers, follow_redirects=False)
            meta_headers = {
                k: v for k, v in meta_resp.headers.items()
                if k.lower() in [h.lower() for h in rule.head_meta_headers]
            }

    async with http_client.stream(
        request.method, target_url, headers=headers,
        content=await request.body(),
        follow_redirects=True
    ) as resp:
        content = await resp.aread()
        content_type = resp.headers.get('content-type', '')

        # 根据 Content-Type 改写内容中的 URL（使用全局配置）
        content = _rewrite_content_urls(content, content_type)

        # 过滤掉可能导致问题的响应头
        response_headers = {k: v for k, v in resp.headers.items()
                           if k.lower() not in ['content-length', 'transfer-encoding', 'content-encoding']}

        # 合并预获取的元数据头部
        response_headers.update(meta_headers)

        return Response(
            content=content,
            status_code=resp.status_code,
            headers=response_headers
        )


def _rewrite_content_urls(content: bytes, content_type: str) -> bytes:
    """改写响应内容中的上游 URL 为代理 URL（使用全局 content_rewrite 配置）
    
    Args:
        content: 响应内容
        content_type: 响应的 Content-Type
    """
    # 获取全局 content_rewrite 配置
    global_rewrite = config.get('content_rewrite', {})
    if not global_rewrite:
        return content
    
    # 检查 Content-Type 是否匹配（支持 HTML 和 JSON）
    content_types = global_rewrite.get('content_types', [])
    if not any(ct in content_type for ct in content_types):
        return content
    
    # 获取要替换的目标 host 列表
    targets = global_rewrite.get('targets', [])
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
    logging.info(f"Rule: name={rule.name}, strategy={rule.strategy}, chunk_size={rule.chunk_size}, concurrency={rule.concurrency}")
    
    # 1. 先 HEAD 获取文件大小和内容类型，跟随重定向
    # 带上客户端的 Authorization header（用于 Docker Registry 认证）
    headers = {}
    auth_header = request.headers.get('authorization')
    if auth_header:
        headers['Authorization'] = auth_header
    
    # 使用独立的 HEAD 客户端，避免占用全局连接池
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0), proxy=upstream_proxy) as head_client:
        head_resp = await head_client.head(target_url, headers=headers, follow_redirects=True)
        # 获取最终 URL（如果有重定向）
        final_url = str(head_resp.url)
        content_length = int(head_resp.headers.get('content-length', 0))
        content_type = head_resp.headers.get('content-type', '')
    logging.info(f"Final URL after redirect: {final_url}")
    logging.info(f"HEAD response: content_length={content_length}, content_type={content_type}")
    
    # 检查文件大小是否符合规则
    if content_length < rule.min_size:
        return await _proxy_request(request, target_url, rule)
    
    # 2. 确定缓存 key
    cache_key_source = getattr(rule, 'cache_key_source', 'final')
    cache_key = target_url if cache_key_source == 'original' else final_url
    
    # 3. 检查缓存
    cached = cache.get(cache_key, content_type)
    if cached:
        logging.info(f"Cache HIT: {cache_key}")
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
            return Response(
                status_code=302,
                headers={
                    "Location": str(request.url),
                    "Cache-Control": "no-cache, no-store, must-revalidate"
                }
            )
        
        # 还在下载中，长轮询等待（最多20秒）
        elapsed = asyncio.get_event_loop().time() - start_time
        logging.info(f"File downloading, long polling: {cache_key} (elapsed: {elapsed:.1f}s)")
        
        # 长轮询：每0.5秒检查一次，最多20秒
        max_wait = 20  # 最大等待20秒
        poll_interval = 0.5
        poll_count = int(max_wait / poll_interval)
        
        for _ in range(poll_count):
            await asyncio.sleep(poll_interval)
            
            # 检查下载是否完成
            if download_task.done():
                exc = download_task.exception()
                if exc:
                    raise HTTPException(502, f"Download failed: {str(exc)}")
                # 等待缓存写入完成（最多5秒）
                for _ in range(10):
                    cached = cache.get(cache_key, content_type)
                    if cached:
                        logging.info(f"Download ready after polling: {cache_key}")
                        return StreamingResponse(
                            _file_iterator(cached),
                            media_type=content_type,
                            headers={"Content-Length": str(os.path.getsize(cached))}
                        )
                    await asyncio.sleep(0.5)
                # 下载完成但缓存还没好，返回302重试
                logging.warning(f"Download done but cache not ready after polling: {cache_key}")
                return Response(
                    status_code=302,
                    headers={
                        "Location": str(request.url),
                        "Cache-Control": "no-cache, no-store, must-revalidate"
                    }
                )
        
        # 20秒到了还没完成，返回302让客户端重试
        logging.info(f"Long polling timeout (20s), returning 302: {cache_key}")
        return Response(
            status_code=302,
            headers={
                "Location": str(request.url),
                "Cache-Control": "no-cache, no-store, must-revalidate"
            }
        )
    
    # 5. 使用信号量控制并发，并开始下载
    # 当 chunk_size=0 时，使用 sequential_download_semaphore 限制只能有一个任务
    # 其他情况使用全局 download_semaphore
    semaphore = sequential_download_semaphore if rule.chunk_size == 0 else download_semaphore
    async with semaphore:
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
            # 创建下载器，启用流式模式和断点续传
            chunk_ttl = config.get('cache', {}).get('chunk_ttl_hours', 48)
            downloader = ParallelDownloader(
                url=final_url,
                filepath=temp_file,
                concurrency=rule.concurrency,
                chunk_size=rule.chunk_size,
                proxy=config['server'].get('upstream_proxy'),
                headers=headers if auth_header else None,
                stream_mode=True,  # 启用流式模式
                cache_manager=cache,  # 启用断点续传缓存
                chunk_ttl_hours=chunk_ttl
            )
            
            # 启动后台下载任务
            download_task = asyncio.create_task(
                downloader.download_with_streaming(cache_key, temp_file, cache, content_type)
            )
            
            # 标记为正在下载（存储文件路径、任务和开始时间）
            start_time = asyncio.get_event_loop().time()
            active_downloads[cache_key] = (temp_file, download_task, start_time)
            
            # 检查是否高负载（信号量被锁定表示并发已满）
            # 根据 chunk_size 选择正确的信号量进行判断
            current_semaphore = sequential_download_semaphore if rule.chunk_size == 0 else download_semaphore
            if current_semaphore.locked():
                # 高负载：立即返回 302 + 10s 延时，快速处理连接队列
                logging.info(f"High load, returning 302 with 10s retry: {cache_key}")
                return Response(
                status_code=302,
                headers={
                    "Location": str(request.url),
                    "Cache-Control": "no-cache, no-store, must-revalidate"
                }
                )
            
            # 非高负载：每0.5秒检测一次，下载完成立即返回，最多等待10秒
            try:
                for _ in range(20):  # 20 * 0.5 = 10秒
                    await asyncio.sleep(0.5)
                    
                    # 检查下载是否完成
                    if download_task.done():
                        exc = download_task.exception()
                        if exc:
                            raise HTTPException(502, f"Download failed: {str(exc)}")
                        # 等待缓存写入（最多5秒）
                        for _ in range(10):
                            cached = cache.get(cache_key, content_type)
                            if cached:
                                logging.info(f"Download ready, returning 200: {cache_key}")
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
    match_result = router.match('/' + full_path, content_length=None)
    if match_result:
        rule, processed_path = match_result
    else:
        rule, processed_path = None, '/' + full_path
    logging.info(f"Matched rule: {rule.name if rule else None}, strategy: {rule.strategy if rule else None}, processed_path: {processed_path}")
    
    # === Handler 特殊处理 ===
    # 如果规则配置了 handler，先调用 handler 的 exec_path 函数
    if rule and rule.handler:
        logging.info(f"Loading handler: {rule.handler}")
        handler_module = load_handler(rule.handler)
        if handler_module:
            try:
                handled, response = await handler_module.exec_path(request, full_path, config, http_client)
                if handled:
                    logging.info(f"Handler {rule.handler} processed the request")
                    return response
                # 未处理，继续后续流程
                logging.info(f"Handler {rule.handler} did not handle the request, continuing")
            except Exception as e:
                logging.error(f"Handler {rule.handler} error: {e}")
                raise HTTPException(502, f"Handler error: {str(e)}")
    # === Handler 特殊处理结束 ===
    
    if not rule:
        raise HTTPException(404, "No matching rule found")
    
    # 构建目标 URL（使用处理后的路径）
    target_url = f"{rule.upstream}{processed_path}"
    
    logging.info(f"Target URL: {target_url}")
    
    if rule.strategy == 'proxy':
        logging.info("Using proxy strategy")
        return await _proxy_request(request, target_url, rule, processed_path)
    
    if rule.strategy == 'parallel':
        # HEAD 请求只做转发，不进行并行下载
        if request.method == 'HEAD':
            logging.info("HEAD request, using proxy strategy")
            return await _proxy_request(request, target_url, rule, processed_path)
        # 仅 GET 支持并行下载
        if request.method != 'GET':
            logging.info(f"Method {request.method} not supported for parallel, using proxy")
            return await _proxy_request(request, target_url, rule, processed_path)
        logging.info("Using parallel download strategy")
        return await _parallel_download(request, target_url, rule)
    
    # fallback
    return await _proxy_request(request, target_url, rule, processed_path)

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
        log_level="info",
        # 并发优化
        workers=1,  # 单进程多线程模式（适合IO密集型）
        loop="uvloop",  # 使用 uvloop 提高性能
        http="httptools",  # 使用 httptools 提高HTTP解析性能
        # 连接优化 - 支持大规模长轮询连接
        backlog=4096,  # TCP连接队列大小（增大以支持更多排队连接）
        limit_concurrency=2000,  # 最大并发连接数（支持大量长轮询连接hold住）
        limit_max_requests=10000,  # 每个连接最大请求数
        timeout_keep_alive=60,  # 保持连接超时时间（必须大于长轮询20秒）
    )

if __name__ == "__main__":
    main()

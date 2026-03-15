"""
多线程分片下载器 - 支持 HTTP Range 并行下载和断点续传
"""
import os
import logging
import aiohttp
import aiofiles
import asyncio
import hashlib
from typing import Optional, List, Set
from dataclasses import dataclass

@dataclass
class Chunk:
    start: int
    end: int
    data: Optional[bytes] = None
    downloaded: bool = False

class ParallelDownloader:
    def __init__(self, url: str, filepath: str, concurrency: int = 4, 
                 chunk_size: int = 5*1024*1024, proxy: Optional[str] = None,
                 headers: Optional[dict] = None, stream_mode: bool = False,
                 cache_manager = None, chunk_ttl_hours: int = 48):
        self.url = url
        self.filepath = filepath
        self.concurrency = concurrency
        self.chunk_size = chunk_size
        self.proxy = proxy
        self.headers = headers or {}
        self.stream_mode = stream_mode  # 流式模式：边下载边写入文件
        self.total_size = 0
        self.chunks: List[Chunk] = []
        self.cache_manager = cache_manager  # 缓存管理器，用于断点续传
        self.chunk_ttl_hours = chunk_ttl_hours  # 分块缓存有效期（小时）
        
    async def _get_file_size(self, session: aiohttp.ClientSession) -> int:
        """获取文件总大小，检查是否支持 Range"""
        headers = dict(self.headers)
        async with session.head(self.url, headers=headers, allow_redirects=True, proxy=self.proxy) as resp:
            if resp.headers.get('Accept-Ranges') != 'bytes':
                raise ValueError(f"Server does not support Range requests: {self.url}")
            return int(resp.headers.get('Content-Length', 0))
    
    def _split_chunks(self, total_size: int) -> List[Chunk]:
        """分割下载范围"""
        # 自动模式：当 chunk_size <= 0 时，自动计算 chunk_size = 总大小 / concurrency
        if self.chunk_size <= 0:
            chunk_size = total_size // max((self.concurrency - 10), 1) # 避免除零错误,扣除10个线程，避免其他任务导致线程不够
            if chunk_size <= 0:
                chunk_size = total_size  # 防止除零或过小
        else:
            chunk_size = self.chunk_size
        
        chunks = []
        for start in range(0, total_size, chunk_size):
            end = min(start + chunk_size - 1, total_size - 1)
            chunks.append(Chunk(start=start, end=end))
        return chunks
    
    async def _download_chunk(self, session: aiohttp.ClientSession, chunk: Chunk, sem: asyncio.Semaphore, retry: int = 3):
        """下载单个分片，带重试机制和断点续传记录"""
        async with sem:
            # 如果已经下载过，跳过
            if chunk.downloaded:
                logging.debug(f"Chunk {chunk.start}-{chunk.end} already downloaded, skipping")
                return
                
            headers = dict(self.headers)
            headers['Range'] = f'bytes={chunk.start}-{chunk.end}'
            
            for attempt in range(retry):
                try:
                    async with session.get(self.url, headers=headers, proxy=self.proxy) as resp:
                        if resp.status == 206:
                            chunk.data = await resp.read()
                            chunk.downloaded = True
                            # 记录到缓存
                            if self.cache_manager:
                                self.cache_manager.mark_chunk_downloaded(
                                    self.url, self.total_size, chunk.start, chunk.end
                                )
                            return
                        elif resp.status == 200:
                            # 服务器不支持 Range，返回完整内容（只接受第一个分片）
                            if chunk.start == 0:
                                chunk.data = await resp.read()
                                chunk.downloaded = True
                                if self.cache_manager:
                                    self.cache_manager.mark_chunk_downloaded(
                                        self.url, self.total_size, chunk.start, chunk.end
                                    )
                                return
                            raise RuntimeError(f"Server returned 200 instead of 206 for range request")
                        else:
                            raise RuntimeError(f"Chunk download failed: {resp.status}")
                except Exception as e:
                    if attempt < retry - 1:
                        wait_time = 2 ** attempt  # 指数退避
                        logging.warning(f"Chunk {chunk.start}-{chunk.end} download failed (attempt {attempt + 1}), retrying in {wait_time}s: {e}")
                        await asyncio.sleep(wait_time)
                    else:
                        raise RuntimeError(f"Chunk download failed after {retry} attempts: {e}")
    
    async def _verify_digest(self, expected_sha256: Optional[str] = None) -> bool:
        """校验文件完整性"""
        if not expected_sha256:
            return True
        sha256 = hashlib.sha256()
        async with aiofiles.open(self.filepath, 'rb') as f:
            while chunk := await f.read(8*1024*1024):
                sha256.update(chunk)
        return sha256.hexdigest() == expected_sha256
    
    async def download(self, expected_sha256: Optional[str] = None) -> str:
        """执行并行下载（支持断点续传），返回文件路径"""
        # 确保目录存在
        os.makedirs(os.path.dirname(self.filepath) or '.', exist_ok=True)
        
        logging.info(f"Starting parallel download: {self.url}")
        logging.info(f"Concurrency: {self.concurrency}, Chunk size: {self.chunk_size / 1024 / 1024:.1f}MB")
        
        async with aiohttp.ClientSession() as session:
            # 1. 获取文件大小
            self.total_size = await self._get_file_size(session)
            logging.info(f"File size: {self.total_size / 1024 / 1024:.1f}MB")
            
            # 2. 预分配文件空间 (减少碎片)
            async with aiofiles.open(self.filepath, 'wb') as f:
                await f.truncate(self.total_size)
            
            # 3. 分割任务
            self.chunks = self._split_chunks(self.total_size)
            actual_chunk_size = self.chunks[0].end - self.chunks[0].start + 1 if self.chunks else 0
            logging.info(f"Split into {len(self.chunks)} chunks, actual chunk size: {actual_chunk_size / 1024 / 1024:.1f}MB")
            
            # 4. 检查已下载的分块（断点续传）
            downloaded_ranges: Set[tuple] = set()
            if self.cache_manager:
                cached_chunks = self.cache_manager.get_downloaded_chunks(
                    self.url, self.total_size, self.chunk_ttl_hours
                )
                for cached in cached_chunks:
                    downloaded_ranges.add((cached['start'], cached['end']))
                    # 标记 chunk 为已下载
                    for chunk in self.chunks:
                        if chunk.start == cached['start'] and chunk.end == cached['end']:
                            chunk.downloaded = True
                            break
                
                skipped = sum(1 for c in self.chunks if c.downloaded)
                if skipped > 0:
                    logging.info(f"Resuming download: {skipped}/{len(self.chunks)} chunks already cached (within {self.chunk_ttl_hours}h)")
            
            # 5. 并发下载（跳过已下载的）
            sem = asyncio.Semaphore(self.concurrency)
            pending_chunks = [c for c in self.chunks if not c.downloaded]
            
            if pending_chunks:
                tasks = [self._download_chunk(session, chunk, sem) for chunk in pending_chunks]
                
                # 显示进度
                completed = sum(1 for c in self.chunks if c.downloaded)
                total_pending = len(pending_chunks)
                
                for task in asyncio.as_completed(tasks):
                    try:
                        await task
                        completed += 1
                        if total_pending > 0 and (completed % max(1, len(self.chunks) // 10) == 0 or completed == len(self.chunks)):
                            progress = completed / len(self.chunks) * 100
                            logging.info(f"Download progress: {progress:.1f}% ({completed}/{len(self.chunks)} chunks)")
                    except Exception as e:
                        logging.error(f"Chunk download failed: {e}")
                        raise
            else:
                logging.info("All chunks already downloaded, skipping download")
            
            # 6. 按序写入文件（只写入新下载的）
            logging.info("Writing chunks to file...")
            async with aiofiles.open(self.filepath, 'r+b') as f:
                for chunk in sorted(self.chunks, key=lambda c: c.start):
                    if chunk.data is not None:  # 只写入本次下载的数据
                        await f.seek(chunk.start)
                        await f.write(chunk.data)
            
            # 7. 校验
            if expected_sha256:
                logging.info("Verifying file digest...")
                if not await self._verify_digest(expected_sha256):
                    # 校验失败，清除 chunk 缓存，下次重新下载
                    if self.cache_manager:
                        self.cache_manager.clear_chunks_for_url(self.url)
                    os.remove(self.filepath)
                    raise RuntimeError("Digest verification failed")
            
            # 8. 下载成功，清除 chunk 缓存
            if self.cache_manager:
                self.cache_manager.clear_chunks_for_url(self.url)
                logging.info("Cleared chunk cache after successful download")
            
            logging.info(f"Download completed: {self.filepath}")
        
        return self.filepath
    
    async def download_with_streaming(self, cache_key: str, temp_file: str, cache, content_type: str):
        """流式下载 - 边下载边写入文件，支持多个客户端同时读取"""
        import aiofiles
        import asyncio
        
        try:
            logging.info(f"Starting streaming download: {self.url}")
            
            async with aiohttp.ClientSession() as session:
                # 1. 获取文件大小
                self.total_size = await self._get_file_size(session)
                logging.info(f"File size: {self.total_size / 1024 / 1024:.1f}MB")
                
                # 2. 预分配文件空间
                async with aiofiles.open(temp_file, 'wb') as f:
                    await f.truncate(self.total_size)
                
                # 3. 分割任务
                self.chunks = self._split_chunks(self.total_size)
                actual_chunk_size = self.chunks[0].end - self.chunks[0].start + 1 if self.chunks else 0
                logging.info(f"Split into {len(self.chunks)} chunks, actual chunk size: {actual_chunk_size / 1024 / 1024:.1f}MB")
                
                # 4. 并发下载并实时写入
                sem = asyncio.Semaphore(self.concurrency)
                
                async def download_and_write(chunk: Chunk):
                    """下载单个分片并立即写入文件"""
                    async with sem:
                        headers = dict(self.headers)
                        headers['Range'] = f'bytes={chunk.start}-{chunk.end}'
                        
                        for attempt in range(3):
                            try:
                                async with session.get(self.url, headers=headers, proxy=self.proxy) as resp:
                                    if resp.status == 206:
                                        data = await resp.read()
                                        # 立即写入文件
                                        async with aiofiles.open(temp_file, 'r+b') as f:
                                            await f.seek(chunk.start)
                                            await f.write(data)
                                        return
                                    elif resp.status == 200 and chunk.start == 0:
                                        data = await resp.read()
                                        async with aiofiles.open(temp_file, 'r+b') as f:
                                            await f.write(data)
                                        return
                                    else:
                                        raise RuntimeError(f"Chunk download failed: {resp.status}")
                            except Exception as e:
                                if attempt < 2:
                                    await asyncio.sleep(2 ** attempt)
                                else:
                                    raise
                
                # 启动所有下载任务
                tasks = [download_and_write(chunk) for chunk in self.chunks]
                
                # 等待所有下载完成
                completed = 0
                failed_chunks = []
                
                for task in asyncio.as_completed(tasks):
                    try:
                        await task
                        completed += 1
                        if completed % max(1, len(self.chunks) // 10) == 0 or completed == len(self.chunks):
                            progress = completed / len(self.chunks) * 100
                            logging.info(f"Download progress: {progress:.1f}% ({completed}/{len(self.chunks)} chunks)")
                    except Exception as e:
                        logging.error(f"Chunk download failed: {e}")
                        failed_chunks.append(e)
                
                # 检查是否有失败的 chunk
                if failed_chunks:
                    raise RuntimeError(f"Download failed: {len(failed_chunks)}/{len(self.chunks)} chunks failed")
                
                logging.info(f"Streaming download completed: {temp_file}")
                
                # 5. 验证文件完整性
                actual_size = os.path.getsize(temp_file)
                if actual_size != self.total_size:
                    raise RuntimeError(f"File size mismatch: expected {self.total_size}, got {actual_size}")
                
                # 6. 存入缓存
                cache.put(cache_key, temp_file, content_type)
                
        except Exception as e:
            logging.error(f"Streaming download failed: {e}")
            if os.path.exists(temp_file):
                os.remove(temp_file)
            raise
        finally:
            # 清理 active_downloads
            import main
            if cache_key in main.active_downloads:
                del main.active_downloads[cache_key]

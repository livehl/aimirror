"""
多线程分片下载器 - 支持 HTTP Range 并行下载
"""
import os
import logging
import aiohttp
import aiofiles
import asyncio
import hashlib
from typing import Optional, List
from dataclasses import dataclass

@dataclass
class Chunk:
    start: int
    end: int
    data: Optional[bytes] = None

class ParallelDownloader:
    def __init__(self, url: str, filepath: str, concurrency: int = 4, 
                 chunk_size: int = 5*1024*1024, proxy: Optional[str] = None,
                 headers: Optional[dict] = None):
        self.url = url
        self.filepath = filepath
        self.concurrency = concurrency
        self.chunk_size = chunk_size
        self.proxy = proxy
        self.headers = headers or {}
        self.total_size = 0
        self.chunks: List[Chunk] = []
        
    async def _get_file_size(self, session: aiohttp.ClientSession) -> int:
        """获取文件总大小，检查是否支持 Range"""
        headers = dict(self.headers)
        async with session.head(self.url, headers=headers, allow_redirects=True, proxy=self.proxy) as resp:
            if resp.headers.get('Accept-Ranges') != 'bytes':
                raise ValueError(f"Server does not support Range requests: {self.url}")
            return int(resp.headers.get('Content-Length', 0))
    
    def _split_chunks(self, total_size: int) -> List[Chunk]:
        """分割下载范围"""
        chunks = []
        for start in range(0, total_size, self.chunk_size):
            end = min(start + self.chunk_size - 1, total_size - 1)
            chunks.append(Chunk(start=start, end=end))
        return chunks
    
    async def _download_chunk(self, session: aiohttp.ClientSession, chunk: Chunk, sem: asyncio.Semaphore, retry: int = 3):
        """下载单个分片，带重试机制"""
        async with sem:
            headers = dict(self.headers)
            headers['Range'] = f'bytes={chunk.start}-{chunk.end}'
            
            for attempt in range(retry):
                try:
                    async with session.get(self.url, headers=headers, proxy=self.proxy) as resp:
                        if resp.status == 206:
                            chunk.data = await resp.read()
                            return
                        elif resp.status == 200:
                            # 服务器不支持 Range，返回完整内容（只接受第一个分片）
                            if chunk.start == 0:
                                chunk.data = await resp.read()
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
        """执行并行下载，返回文件路径"""
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
            logging.info(f"Split into {len(self.chunks)} chunks")
            
            # 4. 并发下载
            sem = asyncio.Semaphore(self.concurrency)
            tasks = [self._download_chunk(session, chunk, sem) for chunk in self.chunks]
            
            # 显示进度
            completed = 0
            for task in asyncio.as_completed(tasks):
                await task
                completed += 1
                if completed % max(1, len(self.chunks) // 10) == 0 or completed == len(self.chunks):
                    progress = completed / len(self.chunks) * 100
                    logging.info(f"Download progress: {progress:.1f}% ({completed}/{len(self.chunks)} chunks)")
            
            # 5. 按序写入文件
            logging.info("Writing chunks to file...")
            async with aiofiles.open(self.filepath, 'r+b') as f:
                for chunk in sorted(self.chunks, key=lambda c: c.start):
                    await f.seek(chunk.start)
                    await f.write(chunk.data)
            
            # 6. 校验
            if expected_sha256:
                logging.info("Verifying file digest...")
                if not await self._verify_digest(expected_sha256):
                    os.remove(self.filepath)
                    raise RuntimeError("Digest verification failed")
            
            logging.info(f"Download completed: {self.filepath}")
        
        return self.filepath

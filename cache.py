"""
缓存管理器 - 基于文件 digest + LRU 淘汰
支持分块下载的断点续传缓存
"""
import os
import hashlib
import sqlite3
import shutil
import asyncio
import json
from pathlib import Path
from typing import Optional, List, Dict
from datetime import datetime, timedelta

class CacheManager:
    def __init__(self, cache_dir: str, max_size_gb: float = 100):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_bytes = int(max_size_gb * 1024**3)
        self.db_path = self.cache_dir / "meta.db"
        self._init_db()
    
    def _init_db(self):
        """初始化 SQLite 元数据表"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        # 文件缓存表
        c.execute('''
            CREATE TABLE IF NOT EXISTS files (
                digest TEXT PRIMARY KEY,
                filepath TEXT NOT NULL,
                size INTEGER NOT NULL,
                accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_accessed ON files(accessed)')
        
        # 分块下载缓存表
        c.execute('''
            CREATE TABLE IF NOT EXISTS chunks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                total_size INTEGER NOT NULL,
                chunk_start INTEGER NOT NULL,
                chunk_end INTEGER NOT NULL,
                downloaded INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(url, chunk_start, chunk_end)
            )
        ''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_chunks_url ON chunks(url)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_chunks_updated ON chunks(updated_at)')
        
        conn.commit()
        conn.close()
    
    def _get_digest(self, url: str, content_type: str = "") -> str:
        """生成缓存键：url 的 sha256（content_type 不参与，避免影响缓存命中）"""
        return hashlib.sha256(url.encode()).hexdigest()
    
    def get(self, url: str, content_type: str = "") -> Optional[str]:
        """获取缓存文件路径，不存在返回 None"""
        digest = self._get_digest(url, content_type)
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT filepath, size FROM files WHERE digest=?', (digest,))
        row = c.fetchone()
        conn.close()
        
        if row and os.path.exists(row[0]):
            # 更新访问时间
            conn = sqlite3.connect(self.db_path)
            conn.execute('UPDATE files SET accessed=CURRENT_TIMESTAMP WHERE digest=?', (digest,))
            conn.commit()
            conn.close()
            return row[0]
        return None
    
    def put(self, url: str, filepath: str, content_type: str = "") -> str:
        """存入缓存，返回 digest"""
        size = os.path.getsize(filepath)
        digest = self._get_digest(url, content_type)
        
        # 目标路径
        target = self.cache_dir / digest
        if not os.path.exists(target):
            try:
                os.link(filepath, target)  # 硬链接，节省空间
            except OSError:
                # 跨文件系统时硬链接失败，使用复制
                shutil.copy2(filepath, target)
        
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''
            INSERT OR REPLACE INTO files (digest, filepath, size, accessed, created)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ''', (digest, str(target), size))
        conn.commit()
        conn.close()
        
        # 检查并执行 LRU 淘汰
        self._evict_if_needed()
        return digest
    
    def _evict_if_needed(self):
        """LRU 淘汰策略"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        # 计算当前总大小
        c.execute('SELECT SUM(size) FROM files')
        total = c.fetchone()[0] or 0
        
        while total > self.max_bytes:
            # 找出最久未访问的文件
            c.execute('''
                SELECT digest, filepath, size FROM files 
                ORDER BY accessed ASC LIMIT 1
            ''')
            row = c.fetchone()
            if not row:
                break
            digest, filepath, size = row
            
            # 删除文件
            if os.path.exists(filepath):
                os.remove(filepath)
            c.execute('DELETE FROM files WHERE digest=?', (digest,))
            total -= size
        
        conn.commit()
        conn.close()
    
    def get_stats(self) -> dict:
        """返回缓存统计"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT COUNT(*), SUM(size), MIN(created), MAX(accessed) FROM files')
        count, size, first, last = c.fetchone()
        conn.close()
        return {
            "count": count or 0,
            "size_bytes": size or 0,
            "size_gb": (size or 0) / 1024**3,
            "first_cached": first,
            "last_accessed": last
        }
    
    # ========== 分块下载缓存管理 ==========
    
    def get_downloaded_chunks(self, url: str, total_size: int, chunk_ttl_hours: int = 48) -> List[Dict]:
        """
        获取已下载的分块列表
        只返回在有效期内且 total_size 匹配的 chunk
        """
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        # 计算过期时间
        expire_time = datetime.now() - timedelta(hours=chunk_ttl_hours)
        
        c.execute('''
            SELECT chunk_start, chunk_end, downloaded, updated_at FROM chunks
            WHERE url = ? AND total_size = ? AND updated_at > ? AND downloaded = 1
            ORDER BY chunk_start
        ''', (url, total_size, expire_time.isoformat()))
        
        rows = c.fetchall()
        conn.close()
        
        chunks = []
        for row in rows:
            chunks.append({
                'start': row[0],
                'end': row[1],
                'downloaded': row[2] == 1,
                'updated_at': row[3]
            })
        return chunks
    
    def mark_chunk_downloaded(self, url: str, total_size: int, chunk_start: int, chunk_end: int):
        """标记分块已下载完成"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''
            INSERT INTO chunks (url, total_size, chunk_start, chunk_end, downloaded, updated_at)
            VALUES (?, ?, ?, ?, 1, CURRENT_TIMESTAMP)
            ON CONFLICT(url, chunk_start, chunk_end) DO UPDATE SET
                downloaded = 1,
                updated_at = CURRENT_TIMESTAMP
        ''', (url, total_size, chunk_start, chunk_end))
        conn.commit()
        conn.close()
    
    def mark_chunk_pending(self, url: str, total_size: int, chunk_start: int, chunk_end: int):
        """标记分块为待下载状态（用于恢复下载时）"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''
            INSERT INTO chunks (url, total_size, chunk_start, chunk_end, downloaded, updated_at)
            VALUES (?, ?, ?, ?, 0, CURRENT_TIMESTAMP)
            ON CONFLICT(url, chunk_start, chunk_end) DO UPDATE SET
                downloaded = 0,
                updated_at = CURRENT_TIMESTAMP
        ''', (url, total_size, chunk_start, chunk_end))
        conn.commit()
        conn.close()
    
    def clear_chunks_for_url(self, url: str):
        """清除指定 URL 的所有分块记录"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('DELETE FROM chunks WHERE url = ?', (url,))
        conn.commit()
        conn.close()
    
    def cleanup_expired_chunks(self, chunk_ttl_hours: int = 48):
        """清理过期的分块记录"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        expire_time = datetime.now() - timedelta(hours=chunk_ttl_hours)
        c.execute('DELETE FROM chunks WHERE updated_at < ?', (expire_time.isoformat(),))
        deleted = c.rowcount
        conn.commit()
        conn.close()
        return deleted

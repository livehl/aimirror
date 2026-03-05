"""
缓存管理器 - 基于文件 digest + LRU 淘汰
"""
import os
import hashlib
import sqlite3
import shutil
import asyncio
from pathlib import Path
from typing import Optional
from datetime import datetime

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

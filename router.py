"""
路由匹配器 - 根据 URL 规则选择下载策略
"""
import re
from typing import Optional, Dict, Any
from dataclasses import dataclass

@dataclass
class Rule:
    name: str
    pattern: str
    strategy: str  # 'proxy' or 'parallel'
    min_size: int = 0
    max_size: Optional[int] = None
    concurrency: int = 4
    chunk_size: int = 5*1024*1024
    cache_key_source: str = 'final'  # 'final' 或 'original'，用于决定缓存 key 来源
    
    def __post_init__(self):
        self._regex = re.compile(self.pattern)
    
    def match(self, path: str) -> bool:
        return bool(self._regex.search(path))

class Router:
    def __init__(self, rules: list):
        self.rules = [Rule(**r) for r in rules]
    
    def match(self, path: str, content_length: Optional[int] = None) -> Optional[Rule]:
        """匹配第一条符合的规则，并检查大小约束"""
        for rule in self.rules:
            if rule.match(path):
                if content_length is not None:
                    if content_length < rule.min_size:
                        continue
                    if rule.max_size and content_length > rule.max_size:
                        continue
                return rule
        return None
    
    def get_default(self) -> Rule:
        """返回默认规则（通常是 proxy）"""
        for rule in self.rules:
            if rule.name == "default":
                return rule
        return self.rules[-1]

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
    upstream: str  # 上游源 base URL
    strategy: str  # 'proxy' or 'parallel'
    min_size: int = 0
    max_size: Optional[int] = None
    concurrency: int = 4
    chunk_size: int = 5*1024*1024
    cache_key_source: str = 'final'  # 'final' 或 'original'，用于决定缓存 key 来源
    path_rewrite: Optional[list] = None  # 路径重写规则 [{"search": "...", "replace": "..."}]
    content_rewrite: Optional[dict] = None  # 响应内容改写配置 {"content_types": ["text/html"], "targets": ["https://files.pythonhosted.org"]}
    header_rewrite: Optional[dict] = None  # 响应头改写配置 {"realm": "https://auth.docker.io/token", "service": "registry.docker.io"}，使用全局 public_host
    
    def __post_init__(self):
        self._regex = re.compile(self.pattern)
    
    def match(self, path: str) -> bool:
        return bool(self._regex.search(path))
    
    def build_target_url(self, path: str) -> str:
        """构建目标 URL，应用路径重写规则"""
        rewritten_path = path
        if self.path_rewrite:
            for rule in self.path_rewrite:
                rewritten_path = rewritten_path.replace(rule['search'], rule['replace'])
        return f"{self.upstream}{rewritten_path}"

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

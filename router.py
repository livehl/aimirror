"""
路由匹配器 - 根据 URL 规则选择下载策略
支持路径前缀匹配和路径重写
"""
import re
from typing import Optional, Dict, Any, Tuple
from dataclasses import dataclass, field

@dataclass
class Rule:
    name: str
    upstream: str  # 上游源 base URL
    strategy: str  # 'proxy' or 'parallel'
    # 匹配方式（互斥）
    pattern: Optional[str] = None  # 正则匹配模式
    path_prefix: Optional[str] = None  # 路径前缀匹配（如 "/simple"）
    # 路径处理
    strip_prefix: bool = False  # 是否移除匹配的路径前缀
    path_rewrite: Optional[list] = None  # 路径重写规则 [{"search": "...", "replace": "..."}]
    # 下载参数
    min_size: int = 0
    max_size: Optional[int] = None
    concurrency: int = 4
    chunk_size: int = 5*1024*1024
    cache_key_source: str = 'final'  # 'final' 或 'original'，用于决定缓存 key 来源
    # 内容改写（已废弃，使用全局配置）
    content_rewrite: Optional[dict] = None  # 保留兼容旧配置
    handler: Optional[str] = None  # 特殊处理模块路径，如 "handlers.docker"
    head_meta_headers: Optional[list] = None  # HEAD 请求时需要额外保留的响应头列表
    
    def __post_init__(self):
        if self.pattern:
            self._regex = re.compile(self.pattern)
        else:
            self._regex = None
        # 确保 path_prefix 以 / 开头
        if self.path_prefix and not self.path_prefix.startswith('/'):
            self.path_prefix = '/' + self.path_prefix
    
    def match(self, path: str) -> Tuple[bool, str]:
        """
        匹配路径，返回 (是否匹配, 处理后的路径)
        如果 strip_prefix=True，返回的路径会移除前缀
        """
        matched = False
        matched_prefix = ""
        
        if self.path_prefix:
            # 前缀匹配
            if path.startswith(self.path_prefix) or (self.path_prefix.endswith('/') and path.startswith(self.path_prefix.rstrip('/'))):
                matched = True
                matched_prefix = self.path_prefix
        elif self._regex:
            # 正则匹配
            if self._regex.search(path):
                matched = True
        else:
            # 无匹配条件，默认匹配所有
            matched = True
        
        if not matched:
            return False, path
        
        # 处理路径
        processed_path = path
        if self.strip_prefix and matched_prefix:
            processed_path = path[len(matched_prefix):]
            if not processed_path.startswith('/'):
                processed_path = '/' + processed_path
        
        # 应用额外的 path_rewrite
        if self.path_rewrite:
            for rule in self.path_rewrite:
                processed_path = processed_path.replace(rule['search'], rule['replace'])
        
        return True, processed_path
    
    def build_target_url(self, path: str) -> str:
        """构建目标 URL，先进行匹配处理，再拼接 upstream"""
        matched, processed_path = self.match(path)
        if not matched:
            processed_path = path
        return f"{self.upstream}{processed_path}"

class Router:
    def __init__(self, rules: list):
        self.rules = [Rule(**r) for r in rules]
    
    def match(self, path: str, content_length: Optional[int] = None) -> Optional[Tuple[Rule, str]]:
        """
        匹配第一条符合的规则，并检查大小约束
        返回 (规则, 处理后的路径) 或 None
        """
        for rule in self.rules:
            matched, processed_path = rule.match(path)
            if matched:
                if content_length is not None:
                    if content_length < rule.min_size:
                        continue
                    if rule.max_size and content_length > rule.max_size:
                        continue
                return rule, processed_path
        return None
    
    def get_default(self) -> Rule:
        """返回默认规则（通常是 proxy）"""
        for rule in self.rules:
            if rule.name == "default":
                return rule
        return self.rules[-1]

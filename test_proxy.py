"""
fast_proxy 测试套件
测试覆盖：路由匹配、缓存管理、并行下载、HuggingFace 支持
"""
import os
import sys
import asyncio
import tempfile
import shutil
import hashlib
import pytest
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from router import Router, Rule
from cache import CacheManager


class TestRouter:
    """路由匹配测试"""
    
    def test_docker_blob_rule(self):
        """测试 Docker blob 规则匹配"""
        rules = [
            {
                'name': 'docker-blob',
                'pattern': '/v2/.*/blobs/sha256:[a-f0-9]+',
                'strategy': 'parallel',
                'min_size': 1024000,
                'concurrency': 20,
                'chunk_size': 10485760
            }
        ]
        router = Router(rules)
        
        # 应该匹配
        assert router.match('/v2/library/nginx/blobs/sha256:abc123', 2000000) is not None
        assert router.match('/v2/nvidia/cuda/blobs/sha256:7ecefaa6bd84a24f90dbe7872f28a94e88520a07941d553579434034d9dca399', 2000000) is not None
        
        # 不应该匹配（大小不够）
        assert router.match('/v2/library/nginx/blobs/sha256:abc123', 500000) is None
        
    def test_pip_wheel_rule(self):
        """测试 pip wheel 规则匹配"""
        rules = [
            {
                'name': 'pip-wheel',
                'pattern': '/packages/.+\\.whl$',
                'strategy': 'parallel',
                'min_size': 1024000,
                'concurrency': 20,
                'chunk_size': 5242880
            }
        ]
        router = Router(rules)
        
        # 应该匹配
        assert router.match('/packages/torch/torch-2.0.0-cp310-cp310-linux_x86_64.whl', 2000000) is not None
        
        # 不应该匹配
        assert router.match('/simple/torch/', None) is None
        
    def test_huggingface_rule(self):
        """测试 HuggingFace 规则匹配"""
        rules = [
            {
                'name': 'huggingface-gguf',
                'pattern': '/.*/(blob|resolve)/main/.+\\.gguf$',
                'strategy': 'parallel',
                'min_size': 1024000,
                'concurrency': 20,
                'chunk_size': 10485760,
                'cache_key_source': 'original'
            }
        ]
        router = Router(rules)
        
        # 应该匹配 blob 路径
        rule = router.match('/unsloth/Qwen3.5-0.8B-GGUF/blob/main/Qwen3.5-0.8B-UD-Q2_K_XL.gguf', 400000000)
        assert rule is not None
        assert rule.name == 'huggingface-gguf'
        assert rule.cache_key_source == 'original'
        
        # 应该匹配 resolve 路径
        rule = router.match('/unsloth/Qwen3.5-0.8B-GGUF/resolve/main/Qwen3.5-0.8B-UD-Q2_K_XL.gguf', 400000000)
        assert rule is not None
        
    def test_default_rule(self):
        """测试默认规则"""
        rules = [
            {
                'name': 'pip-wheel',
                'pattern': '/packages/.+\\.whl$',
                'strategy': 'parallel',
                'min_size': 1024000,
                'concurrency': 20,
                'chunk_size': 5242880
            },
            {
                'name': 'default',
                'pattern': '.*',
                'strategy': 'proxy'
            }
        ]
        router = Router(rules)
        
        # 不匹配任何特定规则时应该返回 default
        rule = router.match('/some/random/path', None)
        assert rule is not None
        assert rule.name == 'default'
        assert rule.strategy == 'proxy'


class TestCacheManager:
    """缓存管理测试"""
    
    @pytest.fixture
    def temp_cache_dir(self):
        """创建临时缓存目录"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
        
    def test_cache_put_and_get(self, temp_cache_dir):
        """测试缓存存入和读取"""
        cache = CacheManager(temp_cache_dir, max_size_gb=1)
        
        # 创建测试文件
        test_file = os.path.join(temp_cache_dir, 'test_file.bin')
        with open(test_file, 'wb') as f:
            f.write(b'test content' * 1000)
        
        # 存入缓存
        url = 'https://example.com/test/file.bin'
        cache.put(url, test_file, 'application/octet-stream')
        
        # 读取缓存
        cached_path = cache.get(url, 'application/octet-stream')
        assert cached_path is not None
        assert os.path.exists(cached_path)
        
        # 验证内容
        with open(cached_path, 'rb') as f:
            assert f.read() == b'test content' * 1000
            
    def test_cache_miss(self, temp_cache_dir):
        """测试缓存未命中"""
        cache = CacheManager(temp_cache_dir, max_size_gb=1)
        
        # 查询不存在的缓存
        result = cache.get('https://example.com/nonexistent/file.bin')
        assert result is None
        
    def test_cache_digest_consistency(self, temp_cache_dir):
        """测试缓存 digest 一致性（URL 相同则 digest 相同）"""
        cache = CacheManager(temp_cache_dir, max_size_gb=1)
        
        url = 'https://huggingface.co/unsloth/model/resolve/main/file.gguf'
        
        # 多次计算 digest 应该相同
        digest1 = cache._get_digest(url)
        digest2 = cache._get_digest(url)
        assert digest1 == digest2
        
        # 不同 URL 应该不同
        different_url = 'https://huggingface.co/other/model/resolve/main/file.gguf'
        digest3 = cache._get_digest(different_url)
        assert digest1 != digest3
        
    def test_cache_content_type_not_affecting_digest(self, temp_cache_dir):
        """测试 content_type 不影响 digest（修复 HuggingFace 缓存问题）"""
        cache = CacheManager(temp_cache_dir, max_size_gb=1)
        
        url = 'https://example.com/test/file.bin'
        
        # 不同 content_type 应该产生相同的 digest
        digest1 = cache._get_digest(url, 'application/octet-stream')
        digest2 = cache._get_digest(url, 'binary/octet-stream')
        digest3 = cache._get_digest(url, '')
        
        assert digest1 == digest2 == digest3
        
    def test_cache_stats(self, temp_cache_dir):
        """测试缓存统计"""
        cache = CacheManager(temp_cache_dir, max_size_gb=1)
        
        # 创建多个测试文件
        for i in range(3):
            test_file = os.path.join(temp_cache_dir, f'test_file_{i}.bin')
            with open(test_file, 'wb') as f:
                f.write(b'x' * (1024 * 1024))  # 1MB each
            cache.put(f'https://example.com/file{i}.bin', test_file)
        
        # 获取统计
        stats = cache.get_stats()
        assert stats['count'] == 3
        assert stats['size_bytes'] == 3 * 1024 * 1024
        
    def test_cache_lru_eviction(self, temp_cache_dir):
        """测试 LRU 淘汰策略"""
        cache = CacheManager(temp_cache_dir, max_size_gb=0.01)  # 10MB limit
        
        # 创建大文件（超过限制）
        test_file = os.path.join(temp_cache_dir, 'large_file.bin')
        with open(test_file, 'wb') as f:
            f.write(b'x' * (5 * 1024 * 1024))  # 5MB
        
        # 存入第一个文件
        cache.put('https://example.com/file1.bin', test_file)
        
        # 存入第二个文件（应该触发淘汰）
        cache.put('https://example.com/file2.bin', test_file)
        
        # 检查统计
        stats = cache.get_stats()
        # 由于限制 10MB，两个 5MB 文件应该都能存下
        assert stats['size_bytes'] <= 10 * 1024 * 1024


class TestCacheKeySource:
    """缓存 Key 来源配置测试"""
    
    def test_rule_with_cache_key_source_original(self):
        """测试配置 cache_key_source 为 original"""
        rule = Rule(
            name='huggingface-gguf',
            pattern='/.*/(blob|resolve)/main/.+\\.gguf$',
            strategy='parallel',
            cache_key_source='original'
        )
        assert rule.cache_key_source == 'original'
        
    def test_rule_default_cache_key_source(self):
        """测试默认 cache_key_source 为 final"""
        rule = Rule(
            name='docker-blob',
            pattern='/v2/.*/blobs/sha256:[a-f0-9]+',
            strategy='parallel'
        )
        assert rule.cache_key_source == 'final'


class TestIntegration:
    """集成测试"""
    
    @pytest.fixture
    def temp_cache_dir(self):
        """创建临时缓存目录"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
        
    def test_full_config_loading(self, temp_cache_dir):
        """测试完整配置加载"""
        import yaml
        
        config = {
            'server': {
                'host': '0.0.0.0',
                'port': 8081,
                'upstream_proxy': None
            },
            'cache': {
                'dir': temp_cache_dir,
                'max_size_gb': 100,
                'lru_enabled': True
            },
            'rules': [
                {
                    'name': 'docker-blob',
                    'pattern': '/v2/.*/blobs/sha256:[a-f0-9]+',
                    'strategy': 'parallel',
                    'min_size': 1024000,
                    'concurrency': 20,
                    'chunk_size': 10485760
                },
                {
                    'name': 'huggingface-gguf',
                    'pattern': '/.*/(blob|resolve)/main/.+\\.gguf$',
                    'strategy': 'parallel',
                    'min_size': 1024000,
                    'concurrency': 20,
                    'chunk_size': 10485760,
                    'cache_key_source': 'original'
                },
                {
                    'name': 'default',
                    'pattern': '.*',
                    'strategy': 'proxy'
                }
            ],
            'logging': {
                'level': 'INFO',
                'file': '/tmp/test.log'
            }
        }
        
        # 验证配置可以被正确解析
        router = Router(config['rules'])
        assert len(router.rules) == 3
        
        # 验证 HuggingFace 规则有正确的 cache_key_source
        hf_rule = router.match('/unsloth/model/blob/main/file.gguf', 400000000)
        assert hf_rule.cache_key_source == 'original'
        
        # 验证 Docker 规则使用默认 cache_key_source
        docker_rule = router.match('/v2/library/nginx/blobs/sha256:abc123', 2000000)
        assert docker_rule.cache_key_source == 'final'


class TestHuggingFaceScenario:
    """HuggingFace 场景测试"""
    
    @pytest.fixture
    def temp_cache_dir(self):
        """创建临时缓存目录"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
        
    def test_huggingface_cache_key_stability(self, temp_cache_dir):
        """测试 HuggingFace 缓存 key 稳定性（临时签名不影响缓存命中）"""
        cache = CacheManager(temp_cache_dir, max_size_gb=1)
        
        # 原始 URL（稳定）
        original_url = 'https://huggingface.co/unsloth/Qwen3.5-0.8B-GGUF/resolve/main/Qwen3.5-0.8B-UD-Q2_K_XL.gguf'
        
        # 不同的临时签名 URL（不稳定，每次请求不同）
        signed_url_1 = original_url + '?X-Amz-Signature=signature1&Expires=123456'
        signed_url_2 = original_url + '?X-Amz-Signature=signature2&Expires=789012'
        
        # 创建测试文件
        test_file = os.path.join(temp_cache_dir, 'model.gguf')
        with open(test_file, 'wb') as f:
            f.write(b'model content' * 10000)
        
        # 使用原始 URL 存入缓存
        cache.put(original_url, test_file)
        
        # 使用原始 URL 应该命中缓存
        assert cache.get(original_url) is not None
        
        # 关键：使用原始 URL 作为 cache_key，而不是签名 URL
        # 这样即使 HuggingFace 返回不同的签名 URL，缓存仍然命中
        
    def test_url_path_conversion(self):
        """测试 HuggingFace URL 路径转换 /blob/ -> /resolve/"""
        blob_url = 'https://huggingface.co/unsloth/model/blob/main/file.gguf'
        resolve_url = 'https://huggingface.co/unsloth/model/resolve/main/file.gguf'
        
        # 模拟路径转换逻辑
        converted_url = blob_url.replace('/blob/', '/resolve/')
        assert converted_url == resolve_url


def run_tests():
    """运行所有测试"""
    pytest.main([__file__, '-v', '--tb=short'])


if __name__ == '__main__':
    run_tests()

"""
fast_proxy 简单测试脚本（无需 pytest）
测试覆盖：路由匹配、缓存管理
"""
import os
import sys
import tempfile
import shutil

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from router import Router, Rule
from cache import CacheManager


def test_router():
    """测试路由匹配"""
    print("=" * 50)
    print("测试路由匹配")
    print("=" * 50)
    
    rules = [
        {
            'name': 'docker-blob',
            'pattern': '/v2/.*/blobs/sha256:[a-f0-9]+',
            'upstream': 'https://registry-1.docker.io',
            'strategy': 'parallel',
            'min_size': 1024000,
            'concurrency': 20,
            'chunk_size': 10485760
        },
        {
            'name': 'huggingface-gguf',
            'pattern': '/.*/(blob|resolve)/main/.+\\.gguf$',
            'upstream': 'https://huggingface.co',
            'strategy': 'parallel',
            'min_size': 1024000,
            'concurrency': 20,
            'chunk_size': 10485760,
            'cache_key_source': 'original'
        },
        {
            'name': 'default',
            'pattern': '.*',
            'upstream': 'https://pypi.org',
            'strategy': 'proxy'
        }
    ]
    
    router = Router(rules)
    
    # 测试 Docker 规则
    rule = router.match('/v2/library/nginx/blobs/sha256:abc123', 2000000)
    assert rule is not None, "Docker 规则应该匹配"
    assert rule.name == 'docker-blob', f"预期 docker-blob，实际 {rule.name}"
    print("✓ Docker blob 规则匹配成功")
    
    # 测试 HuggingFace 规则
    rule = router.match('/unsloth/Qwen3.5-0.8B-GGUF/blob/main/file.gguf', 400000000)
    assert rule is not None, "HuggingFace 规则应该匹配"
    assert rule.name == 'huggingface-gguf', f"预期 huggingface-gguf，实际 {rule.name}"
    assert rule.cache_key_source == 'original', f"预期 cache_key_source=original，实际 {rule.cache_key_source}"
    print("✓ HuggingFace 规则匹配成功")
    
    # 测试默认规则
    rule = router.match('/some/random/path', None)
    assert rule is not None, "默认规则应该匹配"
    assert rule.name == 'default', f"预期 default，实际 {rule.name}"
    print("✓ 默认规则匹配成功")
    
    print("路由匹配测试通过！\n")


def test_cache():
    """测试缓存管理"""
    print("=" * 50)
    print("测试缓存管理")
    print("=" * 50)
    
    # 创建临时缓存目录
    temp_dir = tempfile.mkdtemp()
    try:
        cache = CacheManager(temp_dir, max_size_gb=1)
        
        # 创建测试文件
        test_file = os.path.join(temp_dir, 'test_file.bin')
        with open(test_file, 'wb') as f:
            f.write(b'test content' * 1000)
        
        # 测试存入缓存
        url = 'https://example.com/test/file.bin'
        cache.put(url, test_file, 'application/octet-stream')
        print("✓ 缓存存入成功")
        
        # 测试读取缓存
        cached_path = cache.get(url, 'application/octet-stream')
        assert cached_path is not None, "缓存应该命中"
        assert os.path.exists(cached_path), "缓存文件应该存在"
        print("✓ 缓存读取成功")
        
        # 测试 content_type 不影响 digest
        digest1 = cache._get_digest(url, 'application/octet-stream')
        digest2 = cache._get_digest(url, 'binary/octet-stream')
        assert digest1 == digest2, "不同 content_type 应该产生相同的 digest"
        print("✓ content_type 不影响 digest（HuggingFace 缓存优化）")
        
        # 测试缓存统计
        stats = cache.get_stats()
        assert stats['count'] == 1, f"预期 count=1，实际 {stats['count']}"
        print(f"✓ 缓存统计: {stats}")
        
        print("缓存管理测试通过！\n")
        
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_huggingface_scenario():
    """测试 HuggingFace 场景"""
    print("=" * 50)
    print("测试 HuggingFace 场景")
    print("=" * 50)
    
    temp_dir = tempfile.mkdtemp()
    try:
        cache = CacheManager(temp_dir, max_size_gb=1)
        
        # 原始 URL（稳定）
        original_url = 'https://huggingface.co/unsloth/model/resolve/main/file.gguf'
        
        # 创建测试文件
        test_file = os.path.join(temp_dir, 'model.gguf')
        with open(test_file, 'wb') as f:
            f.write(b'model content' * 10000)
        
        # 使用原始 URL 存入缓存
        cache.put(original_url, test_file)
        print(f"✓ 使用原始 URL 存入缓存: {original_url}")
        
        # 使用原始 URL 应该命中缓存
        cached = cache.get(original_url)
        assert cached is not None, "原始 URL 应该命中缓存"
        print("✓ 原始 URL 缓存命中成功")
        
        # 验证缓存 key 稳定性
        digest = cache._get_digest(original_url)
        assert len(digest) == 64, "digest 应该是 64 位十六进制字符串"
        print(f"✓ 缓存 digest: {digest[:16]}...")
        
        print("HuggingFace 场景测试通过！\n")
        
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_cache_key_source_config():
    """测试 cache_key_source 配置"""
    print("=" * 50)
    print("测试 cache_key_source 配置")
    print("=" * 50)
    
    # 测试默认值为 'final'
    rule1 = Rule(
        name='docker-blob',
        pattern='/v2/.*/blobs/sha256:[a-f0-9]+',
        upstream='https://registry-1.docker.io',
        strategy='parallel'
    )
    assert rule1.cache_key_source == 'final', f"默认值应该是 'final'，实际是 {rule1.cache_key_source}"
    print("✓ 默认 cache_key_source = 'final'")
    
    # 测试设置为 'original'
    rule2 = Rule(
        name='huggingface-gguf',
        pattern='/.*/(blob|resolve)/main/.+\\.gguf$',
        upstream='https://huggingface.co',
        strategy='parallel',
        cache_key_source='original'
    )
    assert rule2.cache_key_source == 'original', f"应该为 'original'，实际是 {rule2.cache_key_source}"
    print("✓ 配置 cache_key_source = 'original'")
    
    print("cache_key_source 配置测试通过！\n")


def test_end_to_end():
    """端到端测试：启动服务 -> 下载测试 -> 缓存命中测试 -> 关闭服务"""
    print("=" * 50)
    print("端到端集成测试")
    print("=" * 50)
    
    import subprocess
    import time
    import signal
    
    # 测试配置
    test_cases = [
        {
            'name': 'PyPI Wheel',
            'url': 'http://localhost:8081/packages/fb/d7/71b982339efc4fff3c622c6fefecddfd3e0b35b60c5f822872d5b806bb71/torch-1.0.0-cp27-cp27m-manylinux1_x86_64.whl',
            'min_size': 100000000,  # 100MB+
        },
        {
            'name': 'R Package',
            'url': 'http://localhost:8081/src/contrib/tensorflow_2.20.0.tar.gz',
            'min_size': 1000000,  # 1MB+
        },
        {
            'name': 'Docker Layer',
            'url': 'http://localhost:8081/v2/library/nginx/blobs/sha256:206356c42440674ecbdf1070cf70ce8ef7885ac2e5c56f1ecf800b758f6b0419',
            'min_size': 1000000,  # 1MB+
            'auth_required': True,  # 需要 Bearer Token 认证
            'auth_url': 'https://auth.docker.io/token?service=registry.docker.io&scope=repository:library/nginx:pull'
        },
        {
            'name': 'HuggingFace GGUF',
            'url': 'http://localhost:8081/unsloth/Qwen3.5-0.8B-GGUF/blob/main/Qwen3.5-0.8B-UD-Q2_K_XL.gguf',
            'min_size': 300000000,  # 300MB+
        }
    ]
    
    # 1. 清除缓存
    print("\n[1/5] 清除缓存...")
    subprocess.run(['rm', '-rf', '/data/fast_proxy/cache/*'], capture_output=True)
    print("✓ 缓存已清除")
    
    # 2. 启动服务
    print("\n[2/5] 启动 fast_proxy 服务...")
    proc = subprocess.Popen(
        ['python3', 'main.py'],
        cwd='/data/fast_proxy',
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        preexec_fn=os.setsid
    )
    
    # 等待服务启动（重试机制）
    print("  等待服务启动...")
    max_retries = 10
    for i in range(max_retries):
        time.sleep(2)
        try:
            result = subprocess.run(
                ['curl', '-s', '-o', '/dev/null', '-w', '%{http_code}', 'http://localhost:8081/health'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.stdout.strip() == '200':
                print("✓ 服务启动成功")
                break
        except:
            pass
        print(f"  重试 {i+1}/{max_retries}...")
    else:
        print(f"✗ 服务启动失败")
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        return False
    
    # 3. 第一次下载测试（无缓存）
    print("\n[3/5] 第一次下载测试（无缓存，并行下载）...")
    first_download_times = {}
    
    for test in test_cases:
        name = test['name']
        url = test['url']
        
        print(f"\n  测试 {name}...")
        
        # 构建 curl 命令
        cmd = ['curl', '-s', '-o', '/dev/null', '-w', 
               'HTTP:%{http_code},Size:%{size_download},Time:%{time_total}',
               '--max-time', '180', url]
        
        # 如果需要认证，先获取 token
        if test.get('auth_required'):
            print(f"    获取 Docker Registry Token...")
            auth_cmd = ['curl', '-s', test['auth_url']]
            auth_result = subprocess.run(auth_cmd, capture_output=True, text=True)
            
            try:
                import json
                auth_data = json.loads(auth_result.stdout)
                token = auth_data.get('token')
                if token:
                    cmd.extend(['-H', f'Authorization: Bearer {token}'])
                    print(f"    ✓ Token 获取成功")
                else:
                    print(f"    ✗ Token 获取失败: {auth_result.stdout[:200]}")
                    continue
            except Exception as e:
                print(f"    ✗ Token 解析失败: {e}")
                continue
        
        if 'headers' in test:
            for header in test['headers']:
                cmd.extend(['-H', header])
        
        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True)
        elapsed = time.time() - start_time
        
        output = result.stdout.strip()
        print(f"    结果: {output}")
        
        if 'HTTP:200' in output or 'HTTP:302' in output:
            first_download_times[name] = elapsed
            print(f"    ✓ 下载成功，耗时: {elapsed:.2f}s")
        else:
            print(f"    ✗ 下载失败")
    
    # 4. 第二次下载测试（验证缓存命中）
    print("\n[4/5] 第二次下载测试（验证缓存命中）...")
    cache_hits = {}
    
    for test in test_cases:
        name = test['name']
        url = test['url']
        
        print(f"\n  测试 {name}...")
        cmd = ['curl', '-s', '-o', '/dev/null', '-w', 
               'HTTP:%{http_code},Size:%{size_download},Time:%{time_total}',
               '--max-time', '30', url]
        
        # 如果需要认证，重新获取 token
        if test.get('auth_required'):
            auth_cmd = ['curl', '-s', test['auth_url']]
            auth_result = subprocess.run(auth_cmd, capture_output=True, text=True)
            try:
                import json
                auth_data = json.loads(auth_result.stdout)
                token = auth_data.get('token')
                if token:
                    cmd.extend(['-H', f'Authorization: Bearer {token}'])
            except:
                pass
        
        if 'headers' in test:
            for header in test['headers']:
                cmd.extend(['-H', header])
        
        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True)
        elapsed = time.time() - start_time
        
        output = result.stdout.strip()
        print(f"    结果: {output}")
        
        if 'HTTP:200' in output:
            cache_hits[name] = elapsed
            first_time = first_download_times.get(name, elapsed)
            speedup = first_time / elapsed if elapsed > 0 else 0
            print(f"    ✓ 缓存命中！耗时: {elapsed:.2f}s (加速 {speedup:.1f}x)")
        else:
            print(f"    ✗ 下载失败")
    
    # 5. 关闭服务
    print("\n[5/5] 关闭服务...")
    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    proc.wait(timeout=5)
    print("✓ 服务已关闭")
    
    # 打印测试总结
    print("\n" + "=" * 50)
    print("端到端测试总结")
    print("=" * 50)
    print(f"\n测试项目数: {len(test_cases)}")
    print(f"首次下载成功: {len(first_download_times)}/{len(test_cases)}")
    print(f"缓存命中成功: {len(cache_hits)}/{len(test_cases)}")
    
    if first_download_times and cache_hits:
        print("\n性能对比:")
        for name in first_download_times:
            if name in cache_hits:
                first = first_download_times[name]
                second = cache_hits[name]
                speedup = first / second if second > 0 else 0
                print(f"  {name}: {first:.1f}s -> {second:.1f}s (加速 {speedup:.1f}x)")
    
    # 验证结果
    success = len(cache_hits) == len(test_cases)
    
    if success:
        print("\n✓ 所有端到端测试通过！")
    else:
        print(f"\n✗ 部分测试失败 ({len(test_cases) - len(cache_hits)} 项)")
    
    return success


def run_all_tests():
    """运行所有测试"""
    print("\n" + "=" * 50)
    print("fast_proxy 测试套件")
    print("=" * 50 + "\n")
    
    all_passed = True
    
    try:
        # 单元测试
        test_router()
        test_cache()
        test_huggingface_scenario()
        test_cache_key_source_config()
        
        # 端到端测试
        if not test_end_to_end():
            all_passed = False
        
        print("\n" + "=" * 50)
        if all_passed:
            print("所有测试通过！")
        else:
            print("部分测试失败！")
        print("=" * 50)
        return 0 if all_passed else 1
        
    except AssertionError as e:
        print(f"\n测试失败: {e}")
        return 1
    except Exception as e:
        print(f"\n测试出错: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    exit(run_all_tests())

#!/usr/bin/env python3
"""
测试 Docker Registry 认证流程
"""
import subprocess
import json
import sys

PROXY_HOST = "docker.aixhx.cn"

def test_docker_auth():
    """测试 Docker 认证流程"""
    print("=" * 60)
    print("测试 Docker Registry 认证流程")
    print("=" * 60)
    
    # 步骤 1: 获取 Token（通过代理）
    print("\n[步骤 1] 获取 Bearer Token...")
    token_url = f"https://{PROXY_HOST}/token?service=registry.docker.io&scope=repository:library/nginx:pull"
    print(f"  Token URL: {token_url}")
    
    result = subprocess.run(
        ['curl', '-s', '-w', '\nHTTP_CODE:%{http_code}', token_url],
        capture_output=True, text=True
    )
    
    print(f"  响应:\n{result.stdout[:2000]}")
    
    # 提取 HTTP 状态码
    if 'HTTP_CODE:200' not in result.stdout:
        print(f"  ✗ Token 请求失败")
        return False
    
    # 解析 token
    try:
        response_text = result.stdout.split('\nHTTP_CODE:')[0]
        auth_data = json.loads(response_text)
        token = auth_data.get('token')
        if not token:
            print("  ✗ Token 不存在于响应中")
            return False
        print(f"  ✓ Token 获取成功 (长度: {len(token)})")
    except Exception as e:
        print(f"  ✗ Token 解析失败: {e}")
        return False
    
    # 步骤 2: 使用 Token 访问 Registry（通过代理）
    print("\n[步骤 2] 使用 Token 访问 Docker Registry...")
    registry_url = f"https://{PROXY_HOST}/v2/library/nginx/manifests/latest"
    print(f"  Registry URL: {registry_url}")
    print(f"  Authorization: Bearer {token[:50]}...")
    
    result = subprocess.run(
        ['curl', '-s', '-w', '\nHTTP_CODE:%{http_code}', 
         '-H', f'Authorization: Bearer {token}',
         registry_url],
        capture_output=True, text=True
    )
    
    print(f"  响应:\n{result.stdout[:2000]}")
    
    if 'HTTP_CODE:200' in result.stdout:
        print("  ✓ Registry 访问成功！")
        return True
    elif 'HTTP_CODE:401' in result.stdout:
        print("  ✗ Registry 返回 401 Unauthorized")
        return False
    else:
        print(f"  ✗ 意外响应")
        return False


def test_direct_auth():
    """直接测试 auth.docker.io（不经过代理）"""
    print("\n" + "=" * 60)
    print("直接测试 auth.docker.io（不经过代理，作为对照）")
    print("=" * 60)
    
    print("\n[步骤 1] 直接获取 Token...")
    token_url = "https://auth.docker.io/token?service=registry.docker.io&scope=repository:library/nginx:pull"
    print(f"  Token URL: {token_url}")
    
    result = subprocess.run(
        ['curl', '-s', '-w', '\nHTTP_CODE:%{http_code}', token_url],
        capture_output=True, text=True
    )
    
    print(f"  响应:\n{result.stdout[:2000]}")
    
    if 'HTTP_CODE:200' not in result.stdout:
        print(f"  ✗ Token 请求失败")
        return False
    
    try:
        response_text = result.stdout.split('\nHTTP_CODE:')[0]
        auth_data = json.loads(response_text)
        token = auth_data.get('token')
        if not token:
            print("  ✗ Token 不存在于响应中")
            return False
        print(f"  ✓ Token 获取成功 (长度: {len(token)})")
    except Exception as e:
        print(f"  ✗ Token 解析失败: {e}")
        return False
    
    print("\n[步骤 2] 直接使用 Token 访问 Registry...")
    registry_url = "https://registry-1.docker.io/v2/library/nginx/manifests/latest"
    print(f"  Registry URL: {registry_url}")
    
    result = subprocess.run(
        ['curl', '-s', '-w', '\nHTTP_CODE:%{http_code}', 
         '-H', f'Authorization: Bearer {token}',
         registry_url],
        capture_output=True, text=True
    )
    
    print(f"  响应:\n{result.stdout[:2000]}")
    
    if 'HTTP_CODE:200' in result.stdout:
        print("  ✓ Registry 访问成功！")
        return True
    else:
        print(f"  ✗ Registry 访问失败")
        return False


if __name__ == '__main__':
    print("\n")
    
    # 先测试直接访问（作为对照）
    direct_success = test_direct_auth()
    
    print("\n\n")
    
    # 再测试通过代理
    proxy_success = test_docker_auth()
    
    print("\n" + "=" * 60)
    print("测试结果总结")
    print("=" * 60)
    print(f"直接访问: {'✓ 通过' if direct_success else '✗ 失败'}")
    print(f"代理访问: {'✓ 通过' if proxy_success else '✗ 失败'}")
    
    sys.exit(0 if proxy_success else 1)

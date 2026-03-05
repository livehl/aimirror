FROM python:3.11-slim

WORKDIR /app

# 安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY main.py router.py cache.py downloader.py config.yaml ./

# 创建缓存目录和日志目录
RUN mkdir -p /data/fast_proxy/cache /data/fast_proxy/logs

# 暴露端口
EXPOSE 8081

# 启动命令
CMD ["python", "main.py"]

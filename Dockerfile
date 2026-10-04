FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir '.[postgres]'
COPY data/sample ./data/sample
ENTRYPOINT ["python", "-m", "enterprise_etl.cli"]
CMD ["--data-dir", "data/sample", "--output-dir", "artifacts"]

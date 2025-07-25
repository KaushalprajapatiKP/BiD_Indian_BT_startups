FROM python:3.9-slim

WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir uv && uv venv && .venv/bin/activate && uv pip install .

COPY . .

ENTRYPOINT ["python", "main.py"]

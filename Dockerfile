FROM python:3.12-slim

# No .pyc files, unbuffered logs (so `docker logs` shows output immediately)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first so this layer is cached until requirements.txt changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the project (see .dockerignore for what is excluded)
COPY . .

# Run as a non-root user; it owns output/ and logs/ so the pipeline can write there
RUN useradd --create-home appuser \
    && mkdir -p output logs \
    && chown -R appuser:appuser /app
USER appuser

# Default: run the pipeline. Override for other commands (e.g. the dashboard).
CMD ["python", "run_pipeline.py", "--config", "config/pipeline.json"]

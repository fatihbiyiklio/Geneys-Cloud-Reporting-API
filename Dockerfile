# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set environment variables
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
    STREAMLIT_SERVER_USAGE_STATS=false \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    GENESYS_MEMORY_LIMIT_MB=800 \
    GENESYS_MEMORY_HARD_LIMIT_MB=1024 \
    GENESYS_MEMORY_CLEANUP_COOLDOWN_SEC=60 \
    GENESYS_MEMORY_RESTART_COOLDOWN_SEC=300

# Set the working directory
WORKDIR /app

# Install system dependencies with better error handling
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first (layer caching)
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application files (selective - avoid secrets)
COPY app.py run_app.py ./
COPY METRICS_REFERENCE.md ./
COPY src/ ./src/
COPY .streamlit/ ./.streamlit/

# Create necessary directories
RUN mkdir -p orgs

# Create a non-root user and switch to it for security
RUN useradd -m streamlituser && \
    chown -R streamlituser:streamlituser /app
USER streamlituser

# Expose Streamlit port
EXPOSE 8501

# Healthcheck - increased timeouts for memory-constrained environments
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Run with Python wrapper (auto-restart)
ENTRYPOINT ["python", "/app/run_app.py"]

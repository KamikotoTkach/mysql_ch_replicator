FROM python:3.12.10-slim-bookworm

WORKDIR /app

# Copy requirements files
COPY requirements.txt requirements-dev.txt ./

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir -r requirements-dev.txt

# Copy the application
COPY . .

# Create directory for binlog data
RUN mkdir -p /app/binlog

# Make the main script executable
RUN chmod +x /app/main.py

# Set the entrypoint to the main script. Invoke Python explicitly so Windows
# checkouts with CRLF line endings do not break the shebang inside Docker.
ENTRYPOINT ["python", "/app/main.py"]

# Default command (can be overridden in docker-compose)
CMD ["--help"]

# --- Stage 1: Builder ---
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.11.0 AS builder

ENV DEBIAN_FRONTEND=noninteractive

# Install Python dependencies in builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip python3-setuptools python3-dev \
 && pip3 install --break-system-packages --no-cache-dir \
    boto3 \
    pystac-client \
    odc-stac \
    shapely \
    rioxarray \
    pandas \
    matplotlib \
 && rm -rf /var/lib/apt/lists/*

# --- Stage 2: Final image ---
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.11.0

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PATH="/usr/local/bin:$PATH"

# Install Python runtime (no dev headers)
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-minimal python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages from builder
COPY --from=builder /usr/local /usr/local

# Remove unnecessary files to shrink image
RUN find /usr/local/lib/python3.*/ -type d -name "__pycache__" -exec rm -rf {} + \
    && find /usr/local/lib/python3.*/ -type d -name "tests" -exec rm -rf {} + \
    && rm -rf /usr/share/doc/* /usr/share/man/* /usr/share/locale/*

# Copy app
WORKDIR /app
COPY worker.py .

ENTRYPOINT ["python3", "worker.py"]

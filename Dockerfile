FROM alpine:3.19

# Automatically set by Docker when building for multiple platforms
ARG TARGETARCH

# Install runtime dependencies for all transfer methods
RUN apk add --no-cache \
    rsync \
    openssh-client \
    bash \
    ca-certificates \
    python3 \
    py3-pip \
    py3-requests

# Copy pre-built malai binary for the target architecture
COPY binaries/${TARGETARCH}/malai /usr/local/bin/malai
RUN chmod +x /usr/local/bin/malai

# Copy entrypoint script that routes to transfer method handlers
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Copy Weaviate copy script
COPY copy_weaviate.py /usr/local/bin/copy_weaviate.py
RUN chmod +x /usr/local/bin/copy_weaviate.py

# Create data directory
RUN mkdir -p /data

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["help"]

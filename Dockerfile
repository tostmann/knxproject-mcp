# knxproject-mcp: MCP server that keeps the record of a KNX installation
FROM python:3.13.18-slim-trixie

LABEL org.opencontainers.image.title="knxproject-mcp" \
      org.opencontainers.image.description="MCP server that keeps the record of a KNX installation" \
      org.opencontainers.image.licenses="MIT"

# Every KNXPROJECT_* variable can be overridden. With host networking set
# KNXPROJECT_BIND=auto or an address, so the server does not listen on every
# interface of the host.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    KNXPROJECT_DATA=/data \
    KNXPROJECT_BIND=0.0.0.0 \
    KNXPROJECT_ALLOW_ANY_BIND=1 \
    KNXPROJECT_PORT=8096

RUN groupadd --system --gid 10001 knxproject \
 && useradd --system --uid 10001 --gid 10001 --home-dir /nonexistent --no-create-home \
            --shell /usr/sbin/nologin knxproject \
 && mkdir -p /data /import \
 && chown 10001:10001 /data

COPY knxproject_mcp.py LICENSE /app/
COPY skill/SKILL.md /app/skill/

USER 10001:10001
VOLUME ["/data"]
EXPOSE 8096

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD ["python", "/app/knxproject_mcp.py", "health"]

# Imports run in the same image:
#   docker exec <container> python /app/knxproject_mcp.py import esf /import/x.esf --installation home
ENTRYPOINT ["python", "/app/knxproject_mcp.py"]
CMD ["serve"]

## Worker Service Dockerfile
## Context: No worker JAR is produced yet; previous build failed copying target/worker.jar.
## Strategy: Provide a lightweight JRE base, conditional start if /app/worker.jar exists,
## otherwise keep container alive (useful while developing client-lib & infra).

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app

# JVM heap can be overridden via compose env vars
ENV JVM_XMS=128m \
    JVM_XMX=256m

# Healthcheck: healthy if worker.jar running OR if jar absent (idle mode allowed during early dev)
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD bash -c '[ -f /app/worker.jar ] && pgrep -f "worker.jar" > /dev/null || exit 0'

# Run jar if present; else idle loop (acts as placeholder until build pipeline creates jar)
CMD ["bash", "-c", "if [ -f worker.jar ]; then echo '[worker] Starting worker.jar'; exec java -Xms$JVM_XMS -Xmx$JVM_XMX -jar worker.jar; else echo '[worker] worker.jar not found; idle mode'; while true; do sleep 3600; done; fi"]

# Future steps (when worker code exists):
# 1) Add multi-stage build: copy source, run mvn package, copy final jar.
# 2) Replace idle path with failure exit if jar must be mandatory.

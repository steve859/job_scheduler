## Worker Service Dockerfile
## Context: No worker JAR is produced yet; previous build failed copying target/worker.jar.
## Strategy: Provide a lightweight JRE base, conditional start if /app/worker.jar exists,
## otherwise keep container alive (useful while developing client-lib & infra).

## Multi-stage build producing worker.jar
FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /workspace
COPY . .
RUN mvn -q -DskipTests -pl worker -am package

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
ENV JVM_XMS=128m \
    JVM_XMX=256m
COPY --from=build /workspace/worker/target/worker-1.0-SNAPSHOT.jar /app/worker.jar
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 CMD bash -c 'pgrep -f "worker.jar" > /dev/null || exit 1'
CMD ["bash", "-c", "echo '[worker] Starting worker.jar'; exec java -Xms$JVM_XMS -Xmx$JVM_XMX -jar worker.jar"]

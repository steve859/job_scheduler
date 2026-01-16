## Scheduler Service Dockerfile

FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /workspace
COPY . .
RUN mvn -q -DskipTests -pl scheduler -am package

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
ENV JVM_XMS=128m \
    JVM_XMX=256m
COPY --from=build /workspace/scheduler/target/scheduler-1.0-SNAPSHOT.jar /app/scheduler.jar
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 CMD bash -c 'pgrep -f "scheduler.jar" > /dev/null || exit 1'
CMD ["bash", "-c", "echo '[scheduler] Starting scheduler.jar'; exec java -Xms$JVM_XMS -Xmx$JVM_XMX -jar scheduler.jar"]

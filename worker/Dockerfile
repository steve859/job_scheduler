FROM eclipse-temurin:17-jdk-jammy

WORKDIR /app
COPY target/worker.jar /app/worker.jar
# provide env example via docker-compose
ENTRYPOINT ["java","-Xms256m","-Xmx512m","-jar","/app/worker.jar"]

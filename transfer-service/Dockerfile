# syntax=docker/dockerfile:1

# Build stage: the full Maven reactor compiles every module. Only the JDK is needed here.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY . .
RUN mvn -B -DskipTests package

# Runtime stage: a Java 21 JRE. No Maven, no compiler, no sources — only the jar.
FROM eclipse-temurin:21-jre
WORKDIR /app
RUN useradd --system --uid 10001 --gid 0 transfer \
    && chown --recursive 10001:0 /app
COPY --from=build /build/transfer-service/target/transfer-service-*.jar /app/transfer-service.jar
USER 10001:0

EXPOSE 8080

# Docker sends SIGTERM; spring shutdown: graceful answers it and finishes in-flight work.
STOPSIGNAL SIGTERM

# The container limit is 512 MB, so the heap is a percentage of it rather than a fixed
# -Xmx, and off-heap Netty buffers and metaspace are capped explicitly as well.
ENTRYPOINT ["java", \
  "-XX:MaxRAMPercentage=60", \
  "-XX:MaxDirectMemorySize=96M", \
  "-XX:MaxMetaspaceSize=128M", \
  "-XX:+ExitOnOutOfMemoryError", \
  "-Djava.security.egd=file:/dev/./urandom", \
  "-jar", "/app/transfer-service.jar"]

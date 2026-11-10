# Build: compile with the project's own Maven Wrapper on JDK 17 (the minimum the exam allows).
FROM eclipse-temurin:17-jdk AS build
WORKDIR /src
COPY .mvn .mvn
COPY mvnw pom.xml ./
RUN ./mvnw -q -B dependency:go-offline
COPY src src
RUN ./mvnw -q -B -DskipTests package && cp target/async-task-queue-lossless-*.jar /app.jar

# Run: JRE only, non-root, H2 files on a volume so tasks survive container restarts.
FROM eclipse-temurin:17-jre
RUN useradd --system --uid 10001 app && mkdir /data && chown app /data
USER app
COPY --from=build /app.jar /app/app.jar
ENV PORT=8080
EXPOSE 8080
VOLUME /data
# Exec form, so java is PID 1 and receives SIGTERM for the graceful stop.
# Give it time: docker stop -t 45 (the engine needs await-running + await-interrupted).
STOPSIGNAL SIGTERM
ENTRYPOINT ["java", "-jar", "/app/app.jar", "--taskqueue.data-dir=/data"]

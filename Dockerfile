# Multi-stage build: compile the fat jar, then bake it into a Spark image for
# the two Kubernetes CronJob-driven maintenance batches. No local JDK/Maven
# needed — the build stage does it, mirroring the old Compose `builder` service.
#
#   docker build -t s3-table-dump:dev --build-arg APP_VERSION="$(git-semver-release version)" .
#
# APP_VERSION feeds Maven's CI-friendly -Drevision (defaults to 0.0.0-SNAPSHOT).

# ---- build -----------------------------------------------------------------
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /src
# Dependency layer cache: resolve deps before copying sources.
COPY pom.xml ./
RUN mvn -q -DskipTests dependency:go-offline || true
COPY src ./src
ARG APP_VERSION=0.0.0-SNAPSHOT
RUN mvn -q -DskipTests -Drevision="${APP_VERSION}" clean package

# ---- runtime ---------------------------------------------------------------
# Pinned to match spark.version / iceberg-spark-runtime-4.0_2.13 in pom.xml.
FROM apache/spark:4.0.1-scala2.13-java21-ubuntu
COPY --from=build /src/target/app.jar /opt/spark/work-dir/app.jar

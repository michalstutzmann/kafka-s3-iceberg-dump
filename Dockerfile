# Multi-stage build: compile the fat jar, then bake it into a Flink 2.0 image
# for Application Mode (the Flink Kubernetes Operator runs this image as a
# dedicated per-job cluster). No local JDK/Maven needed — the build stage does
# it, mirroring the old Compose `builder` service.
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
RUN mvn -q -DskipTests -Drevision="${APP_VERSION}" clean package \
 && mvn -q dependency:copy-dependencies -DincludeArtifactIds=postgresql \
      -DexcludeTransitive=true -DoutputDirectory=/drivers

# ---- runtime ---------------------------------------------------------------
# Pinned to match flink.version / iceberg-flink-runtime-2.0 in pom.xml.
FROM flink:2.0.2-java21
# Application Mode picks the job jar up from /opt/flink/usrlib.
COPY --from=build /src/target/app.jar /opt/flink/usrlib/app.jar
# The PostgreSQL JDBC driver must also be on Flink's SYSTEM classpath
# (/opt/flink/lib), not only inside the shaded app.jar: java.sql.DriverManager
# resolves drivers via the system classloader, but the Iceberg JDBC catalog is
# opened from operators on Flink's user classloader, so a driver only in the
# fat jar yields "No suitable driver found" in Application Mode. Version is
# whatever pom.xml pins (copied from the build, not hardcoded here).
COPY --from=build /drivers/ /opt/flink/lib/

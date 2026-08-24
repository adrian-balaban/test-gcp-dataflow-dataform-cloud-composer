# Target System mock — the Target System stand-in the Loader App pushes to.
#
# Multi-stage: Maven builds the app from the repo root context (so the `common`
# module is available), then only the JRE and the installed app survive into the
# runtime layer. Built automatically by `make up` via local/docker-compose.yml.
#
# ── build ────────────────────────────────────────────────────────────────────
FROM docker.io/maven:3.9-eclipse-temurin-25 AS build

WORKDIR /build

# POMs first so the dependency layer caches across source changes. All five module
# poms are needed even though only two are built: the root pom lists every module,
# and Maven refuses to read the reactor if any declared module is missing.
COPY pom.xml ./
COPY apps/common/pom.xml ./apps/common/pom.xml
COPY apps/extractor-app/pom.xml ./apps/extractor-app/pom.xml
COPY apps/loader-app/pom.xml ./apps/loader-app/pom.xml
COPY apps/recon-service/pom.xml ./apps/recon-service/pom.xml
COPY apps/target-system-mock/pom.xml ./apps/target-system-mock/pom.xml
RUN mvn -B -q -pl apps/target-system-mock -am dependency:go-offline -DskipTests

# Source last — it changes most often.
# apps/common embeds contracts/artefacts.json as a classpath resource; the Maven
# enforcer fails the build without it rather than shipping a jar that dies on first use.
COPY contracts/artefacts.json ./contracts/artefacts.json

COPY apps/common/src ./apps/common/src
COPY apps/target-system-mock/src ./apps/target-system-mock/src

# The shade plugin writes one self-contained jar with every dependency inside.
RUN mvn -B -q -pl apps/target-system-mock -am package -DskipTests

# ── runtime ───────────────────────────────────────────────────────────────────
FROM docker.io/eclipse-temurin:25-jre

# Temurin JRE has no HTTP client on PATH; install curl so the compose healthcheck
# can probe /__admin/health without a separate tool image.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

ENV TARGET_SYSTEM_PORT=8080 \
    TARGET_SYSTEM_FAILURE_RATE=0.15 \
    TARGET_SYSTEM_SEED=20260803

COPY --from=build /build/apps/target-system-mock/target/target-system-mock.jar /opt/target-system-mock.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/opt/target-system-mock.jar"]

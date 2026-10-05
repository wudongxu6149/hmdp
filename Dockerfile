FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY hm-dianping/pom.xml ./pom.xml
COPY hm-dianping/src ./src
RUN mvn -B -ntp -DskipTests package

FROM eclipse-temurin:17-jre-jammy
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /build/target/hm-dianping-0.0.1-SNAPSHOT.jar ./app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

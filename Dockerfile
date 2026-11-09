FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
COPY frontend frontend
COPY src src
RUN mvn -B -DskipTests package
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /src/target/replay-console.jar /app/console.jar
ENTRYPOINT ["java","-Xmx512m","-jar","/app/console.jar"]

FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
COPY src src
RUN mvn -B -DskipTests package
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /src/target/kafka-lab.jar /app/kafka-lab.jar
COPY config /app/config
RUN mkdir -p /app/data
ENTRYPOINT ["java","-Xmx384m","-jar","/app/kafka-lab.jar"]
CMD ["run","/app/config/demo.properties"]

FROM gradle:8.14.3-jdk21
WORKDIR /app
COPY build/libs/kafka-lessons-0.1.0-SNAPSHOT.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

FROM openjdk:21
WORKDIR /app

COPY ./src /app/src

RUN mkdir -p /app/target/classes && \
    javac -d /app/target/classes /app/src/main/java/org/example/ControladorCentralDistribuido.java

CMD ["java", "-cp", "target/classes", "org.example.ControladorCentralDistribuido"]

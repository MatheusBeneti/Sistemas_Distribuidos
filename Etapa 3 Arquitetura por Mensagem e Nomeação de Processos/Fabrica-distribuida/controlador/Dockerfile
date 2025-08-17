FROM openjdk:21-jdk-slim

RUN apt-get update && apt-get install -y maven

WORKDIR /app

# Copiar arquivos do projeto
COPY ./src /app/src
COPY ./pom.xml /app/pom.xml

# Baixar dependências e compilar
RUN mvn clean compile dependency:copy-dependencies

CMD ["java", "-cp", "target/classes:target/dependency/*", "org.example.ControladorCentralKafka"]

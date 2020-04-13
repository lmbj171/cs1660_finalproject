FROM maven:3.6.0-jdk-11-slim
COPY . /usr/src/myapp
COPY finalproject.json finalproject.json
WORKDIR /usr/src/myapp
ENV GOOGLE_APPLICATION_CREDENTIALS "finalproject.json"
RUN mvn package
CMD ["date"]
CMD ["mvn", "exec:java", "-D", "exec.mainClass=finalproject.finalproject.App"]

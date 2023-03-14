FROM maven:3.8.1-jdk-8
COPY . /work/acid
WORKDIR  /work/acid
RUN mvn compile
CMD ['bash']
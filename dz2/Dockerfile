FROM ubuntu:22.04

RUN apt -y update && \
    apt -y install maven openjdk-8-jdk wget && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz && \
    tar -xzvf hadoop-3.3.6.tar.gz && \
    mv hadoop-3.3.6 /opt/hadoop-3.3.6 && \
    rm hadoop-3.3.6.tar.gz

COPY mm /mm
VOLUME /mm

WORKDIR /mm

RUN export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 && \
    export PATH=$JAVA_HOME/bin:$PATH && \
    java -version && mvn -version && \
    mvn -X clean package

ENTRYPOINT ["bash", "-c", "/opt/hadoop-3.3.6/bin/hadoop jar $JAR_FILE $JAVA_CLASS $TASK_ARGS"]

# /opt/hadoop-3.3.6/bin/hadoop jar target/mm.jar mm -conf config.xml /mm/data/A /mm/data/B /mm/data/out
# docker buildx build --platform linux/amd64 -t bigdata_image:latest --load .
# docker --context desktop-linux run --rm -it --entrypoint bash  -v $(pwd)/task2-example-tests/test2:/mm/data bigdata_image
# docker --context desktop-linux run --rm -it --entrypoint bash  -v $(pwd)/task2-tests/task2-test5:/mm/data bigdata_image
# docker --context desktop-linux run --rm -it --entrypoint bash  -v $(pwd)/candle:/candle/out bigdata_image cp -r target/ out/

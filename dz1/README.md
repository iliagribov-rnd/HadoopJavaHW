Для сборки/компиляции программы использовался следующий Dockerfile:

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

    COPY candle /candle
    VOLUME /candle

    WORKDIR /candle

    RUN export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 && \
        export PATH=$JAVA_HOME/bin:$PATH && \
        java -version && mvn -version && \
        mvn -X clean package

    ENTRYPOINT ["bash", "-c", "/opt/hadoop-3.3.6/bin/hadoop jar $JAR_FILE $JAVA_CLASS $TASK_ARGS"]

Со следующей командой:
    docker buildx build --platform linux/amd64 -t bigdata_image:latest --load .

Для запуска программы выполнялись следующие команды:
1) Запуск docker контейнера и подключение к нему командой:
    docker --context desktop-linux run --rm -it --entrypoint bash  -v $(pwd)/task1/test1:/candle/data bigdata_image
2) Постановка на выполнение map-reduce задачи:
    /opt/hadoop-3.3.6/bin/hadoop jar target/candle.jar -conf /candle/data/config.xml /candle/data/input.csv /candle/data/output_my

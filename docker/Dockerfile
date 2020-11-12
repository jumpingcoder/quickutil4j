FROM alpine:3.12.1
RUN apk update
RUN apk add openjdk8 busybox-extras tzdata curl
ENV JAVA_HOME /usr/lib/jvm/java-1.8-openjdk/bin
ENV PATH $PATH:$JAVA_HOME
RUN java -version
RUN cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN echo Asia/Shanghai > /etc/timezone
RUN date
# docker run -it alpine-3-12:openjdk-8 /bin/sh
# docker build -f Dockerfile -t alpine-3-12:openjdk-8 .
# cat /etc/os-release
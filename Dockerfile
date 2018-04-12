FROM ubuntu:16.04

RUN \
   apt-get update \
   && apt-get -y install \
       openjdk-8-jdk \
       git \
       gradle \
       curl \
       graphviz \
       nano \
   && apt-get clean

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 0C49F3730359A14518585931BC711F9BA15703C6
RUN echo "deb [ arch=amd64,arm64 ] http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.4 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-3.4.list
RUN apt-get update
RUN apt-get install -y mongodb-org
RUN mkdir /opt/files
RUN mkdir /data
RUN mkdir /data/db

COPY entrypoint.sh /
RUN chmod u+x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

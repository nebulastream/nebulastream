#!/bin/bash
# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# install and start kafka & zookeeper
echo "\033[0;31mSet up Kafka instance ...\033[0m"
apt-get update -y && apt-get install -y wget openjdk-8-jdk openjdk-8-jre
cd ${HOME}
ZOOKEEPER_DIR=apache-zookeeper-3.5.6-bin
wget http://mirror.softaculous.com/apache/zookeeper/zookeeper-3.5.6/apache-zookeeper-3.5.6-bin.tar.gz -O ${ZOOKEEPER_DIR}.tar.gz && tar -xzf ${ZOOKEEPER_DIR}.tar.gz && cd ${ZOOKEEPER_DIR}
cat > conf/zoo.cfg <<EOF
tickTime=2000
dataDir=./var/lib/zookeeper
clientPort=2181
EOF

bin/zkServer.sh start
cd ${HOME}
KAFKA_DIR=kafka_2.12-2.4.0
wget "http://mirror.dkd.de/apache/kafka/2.4.0/kafka_2.12-2.4.0.tgz" -O ${KAFKA_DIR}.tgz && tar -zxf ${KAFKA_DIR}.tgz && cd ${KAFKA_DIR} && rm -rf /tmp/kafka-logs/
bin/kafka-server-start.sh config/server.properties &> /tmp/kafka-server.log &
sleep 1                         # waiting for kafka server ready
cd ${HOME}
rm -rf ${ZOOKEEPER_DIR}.tar.gz ${KAFKA_DIR}.tgz
echo "\033[0;31mFinished Kafka instance ...\033[0m"

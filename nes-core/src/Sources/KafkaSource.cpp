/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Runtime/QueryManager.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/KafkaSource.hpp>
#include <Util/Logger/Logger.hpp>
#include <cppkafka/cppkafka.h>
#include <cstdint>
#include <cstring>
#include <sstream>
#include <string>

namespace NES {

KafkaSource::KafkaSource(SchemaPtr schema,
                         Runtime::BufferManagerPtr bufferManager,
                         Runtime::QueryManagerPtr queryManager,
                         uint64_t numbersOfBufferToProduce,
                         const std::string brokers,
                         const std::string topic,
                         const std::string groupId,
                         bool autoCommit,
                         uint64_t kafkaConsumerTimeout,
                         OperatorId operatorId,
                         OriginId originId,
                         size_t numSourceLocalBuffers,
                         const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors)
    : DataSource(std::move(schema),
                 std::move(bufferManager),
                 std::move(queryManager),
                 operatorId,
                 originId,
                 numSourceLocalBuffers,
                 GatheringMode::INTERVAL_MODE,
                 std::move(successors)),
      brokers(brokers), topic(topic), groupId(groupId), autoCommit(autoCommit),
      kafkaConsumerTimeout(std::chrono::milliseconds(kafkaConsumerTimeout)) {

    config = std::make_unique<cppkafka::Configuration>();
    config->set("metadata.broker.list", brokers.c_str());
    config->set("group.id", 0);
    config->set("enable.auto.commit", autoCommit == true ? "true" : "false");
    config->set("auto.offset.reset", "earliest");
    this->numBuffersToProcess = numbersOfBufferToProduce;
}

KafkaSource::~KafkaSource() {}

std::optional<Runtime::TupleBuffer> KafkaSource::receiveData() {
    if (!connect()) {
        NES_DEBUG("Connect Kafa Source");
    }
    bool pollSuccessFull = false;
    uint64_t maxPollCnt = 64;
    uint64_t currentPollCnt = 0;
    NES_DEBUG("KAFKASOURCE tries to receive data...");
    while (!pollSuccessFull) {
        cppkafka::Message msg = consumer->poll();
        if (msg) {
            if (msg.get_error()) {
                if (!msg.is_eof()) {
                    NES_WARNING("KAFKASOURCE received error notification: " << msg.get_error());
                }
                return std::nullopt;
            } else {
                Runtime::TupleBuffer buffer = localBufferManager->getBufferBlocking();

                const uint64_t tupleSize = schema->getSchemaSizeInBytes();
                const uint64_t tupleCnt = msg.get_payload().get_size() / tupleSize;
                const uint64_t payloadSize = msg.get_payload().get_size();

                NES_DEBUG("KAFKASOURCE recv #tups: " << tupleCnt << ", tupleSize: " << tupleSize << " payloadSize=" << payloadSize
                          //                                                     << ", msg: " << msg.get_payload()
                );

                std::memcpy(buffer.getBuffer(), msg.get_payload().get_data(), msg.get_payload().get_size());
                buffer.setNumberOfTuples(tupleCnt);

                // XXX: maybe commit message every N times
                if (!autoCommit)
                    consumer->commit(msg);
                return buffer;
            }
        } else {
            NES_DEBUG("Poll NOT successfull for cnt=" << currentPollCnt << " maxPollCnt=" << maxPollCnt);
        }

        currentPollCnt++;
        if (currentPollCnt == maxPollCnt) {
            NES_DEBUG("Poll reached max poll count=" << maxPollCnt);
            pollSuccessFull = true;
        }
    }

    return std::nullopt;
}

std::string KafkaSource::toString() const {
    std::stringstream ss;
    ss << "KAFKA_SOURCE(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    ss << "BROKER(" << brokers << "), ";
    ss << "TOPIC(" << topic << "). ";
    return ss.str();
}

bool KafkaSource::connect() {
    if (!connected) {
        DataSource::open();
        // Construct the configuration
        cppkafka::Configuration config = {{"metadata.broker.list", brokers},
                                          {"group.id", groupId},
                                          {"auto.offset.reset", "earliest"},
                                          // Disable auto commit
                                          {"enable.auto.commit", false}};

        // Create the consumer
        consumer = std::make_unique<cppkafka::Consumer>(config);

        // Print the assigned partitions on assignment
        consumer->set_assignment_callback([](const cppkafka::TopicPartitionList& partitions) {
            NES_DEBUG("Got assigned: " << partitions);
        });

        // Print the revoked partitions on revocation
        consumer->set_revocation_callback([](const cppkafka::TopicPartitionList& partitions) {
            NES_DEBUG("Got revoked: " << partitions);
        });

        // Subscribe to the topic
        consumer->subscribe({topic});

        NES_DEBUG("Consuming messages from topic " << topic << " brocker=" << brokers << " groupid=" << groupId);

        connected = true;
    }
    return connected;
}

SourceType KafkaSource::getType() const { return KAFKA_SOURCE; }

std::string KafkaSource::getBrokers() const { return brokers; }

std::string KafkaSource::getTopic() const { return topic; }

std::string KafkaSource::getGroupId() const { return groupId; }

bool KafkaSource::isAutoCommit() const { return autoCommit; }

const std::chrono::milliseconds& KafkaSource::getKafkaConsumerTimeout() const { return kafkaConsumerTimeout; }

}// namespace NES
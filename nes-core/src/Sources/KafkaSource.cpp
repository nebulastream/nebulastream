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
#ifdef ENABLE_KAFKA_BUILD
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/KafkaSource.hpp>
#include <Util/Logger/Logger.hpp>
#include <cppkafka/cppkafka.h>
#include <cstdint>
#include <cstring>
#include <filesystem>
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
                         std::string offsetMode,
                         OperatorId operatorId,
                         OriginId originId,
                         size_t numSourceLocalBuffers,
                         uint64_t batchSize,
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
      kafkaConsumerTimeout(std::chrono::milliseconds(kafkaConsumerTimeout)), offsetMode(offsetMode), batchSize(batchSize) {

    config = std::make_unique<cppkafka::Configuration>();
    config->set("metadata.broker.list", brokers.c_str());
    config->set("group.id", groupId);
    config->set("enable.auto.commit", autoCommit == true ? "true" : "false");
    config->set("auto.offset.reset", offsetMode);

    this->numBuffersToProcess = numbersOfBufferToProduce;

    numberOfTuplesPerBuffer =
        std::floor(double(localBufferManager->getBufferSize()) / double(this->schema->getSchemaSizeInBytes()));
}

KafkaSource::~KafkaSource() {
    std::cout << "Kafka source " << topic << " partition/group=" << groupId << " produced=" << bufferProducedCnt
              << " batchSize=" << batchSize << " successFullPollCnt=" << successFullPollCnt
              << " failedFullPollCnt=" << failedFullPollCnt << "reuseCnt=" << reuseCnt << std::endl;
}
std::optional<Runtime::TupleBuffer> KafkaSource::receiveData() {
    if (!connect()) {
        NES_DEBUG("Connect Kafa Source");
    }

    uint64_t currentPollCnt = 0;
    NES_DEBUG("KAFKASOURCE tries to receive data...");
    while (true) {
        //iterate over the polled message buffer
        if (!messages.empty()) {
            reuseCnt++;
            if (messages.back().get_error()) {
                if (!messages.back().is_eof()) {
                    NES_WARNING("KAFKASOURCE received error notification: " << messages.back().get_error());
                }
                return std::nullopt;
            } else {
                const uint64_t tupleSize = schema->getSchemaSizeInBytes();
                const uint64_t tupleCnt = messages.back().get_payload().get_size() / tupleSize;
                const uint64_t payloadSize = messages.back().get_payload().get_size();

                NES_TRACE("KAFKASOURCE recv #tups: " << tupleCnt << ", tupleSize: " << tupleSize << " payloadSize=" << payloadSize
                                                     << ", msg: " << messages.back().get_payload());
                auto currentTime = std::chrono::high_resolution_clock::now().time_since_epoch();
                auto timeStamp = std::chrono::duration_cast<std::chrono::milliseconds>(currentTime).count();

                //this method copies the payload of the kafka message into a buffer
                Runtime::TupleBuffer buffer = localBufferManager->getBufferBlocking();
                NES_ASSERT(messages.back().get_payload().get_size() <= buffer.getBufferSize(), "The buffer is not large enough");
                std::memcpy(buffer.getBuffer(),
                            messages.back().get_payload().get_data(),
                            messages.back().get_payload().get_size());
                buffer.setNumberOfTuples(tupleCnt);
                buffer.setCreationTimestamp(timeStamp);
                bufferProducedCnt++;
                messages.pop_back();
                return buffer;

            }//end of else
        } else {
            NES_DEBUG("Poll NOT successfull for cnt=" << currentPollCnt++);

            //poll a batch of messages and put it into a vector
            messages = consumer->poll_batch(batchSize);

            //do bookkeeping make the behavior comprehensible
            if (!messages.empty()) {
                successFullPollCnt++;
            } else {
                failedFullPollCnt++;
            }

            //if the source is requested to stop we
            if (!this->running) {
                NES_DEBUG("Source stops so stop pulling from kafka");
                return std::nullopt;
            }
        }
    }
}

std::string KafkaSource::toString() const {
    std::stringstream ss;
    ss << "KAFKA_SOURCE(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    ss << "BROKER(" << brokers << "), ";
    ss << "TOPIC(" << topic << "). ";
    ss << "OFFSETMODE(" << offsetMode << "). ";
    ss << "BATCHSIZE(" << batchSize << "). ";
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
        std::vector<cppkafka::TopicPartition> vec;
        cppkafka::TopicPartition assignment(topic, std::atoi(groupId.c_str()));
        vec.push_back(assignment);
        consumer->assign(vec);
        NES_DEBUG("kafka source=" << this->operatorId << " connect to topic=" << topic
                                  << " partition=" << std::atoi(groupId.c_str()));

        NES_DEBUG("kafka source starts producing");

        connected = true;
    }
    return connected;
}

SourceType KafkaSource::getType() const { return KAFKA_SOURCE; }

std::string KafkaSource::getBrokers() const { return brokers; }

std::string KafkaSource::getTopic() const { return topic; }

std::string KafkaSource::getOffsetMode() const { return offsetMode; }

std::string KafkaSource::getGroupId() const { return groupId; }

uint64_t KafkaSource::getBatchSize() const { return batchSize; }

bool KafkaSource::isAutoCommit() const { return autoCommit; }

const std::chrono::milliseconds& KafkaSource::getKafkaConsumerTimeout() const { return kafkaConsumerTimeout; }
}// namespace NES
#endif
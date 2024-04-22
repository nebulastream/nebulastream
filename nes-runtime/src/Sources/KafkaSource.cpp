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
#include <API/AttributeField.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/KafkaSource.hpp>
#include <Sources/Parsers/JSONParser.hpp>
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
                         uint64_t numberOfBuffersToProduce,
                         const std::string brokers,
                         const std::string topic,
                         const std::string groupId,
                         bool autoCommit,
                         uint64_t kafkaConsumerTimeout,
                         std::string offsetMode,
                         const KafkaSourceTypePtr& kafkaSourceType,
                         OriginId originId,
                         StatisticId statisticId,
                         OperatorId operatorId,
                         size_t numSourceLocalBuffers,
                         uint64_t batchSize,
                         const std::string& physicalSourceName,
                         const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors)
    : DataSource(schema,
                 std::move(bufferManager),
                 std::move(queryManager),
                 operatorId,
                 originId,
                 statisticId,
                 numSourceLocalBuffers,
                 GatheringMode::INTERVAL_MODE,
                 physicalSourceName,
                 std::move(successors)),
      brokers(brokers), topic(topic), groupId(groupId), autoCommit(autoCommit),
      kafkaConsumerTimeout(std::chrono::milliseconds(kafkaConsumerTimeout)), offsetMode(offsetMode), batchSize(batchSize) {

    config = {{"metadata.broker.list", brokers},
              {"group.id", groupId},
              {"auto.offset.reset", offsetMode},
              // Disable auto commit
              {"enable.auto.commit", false}};

    this->numberOfBuffersToProduce = numberOfBuffersToProduce;

    numberOfTuplesPerBuffer =
        std::floor(double(localBufferManager->getBufferSize()) / double(this->schema->getSchemaSizeInBytes()));

    //init physical types
    std::vector<std::string> schemaKeys;
    std::string fieldName;
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();

    //Extracting the schema keys in order to parse incoming data correctly (e.g. use as keys for JSON objects)
    //Also, extracting the field types in order to parse and cast the values of incoming data to the correct types
    for (const auto& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
        fieldName = field->getName();
        schemaKeys.push_back(fieldName.substr(fieldName.find('$') + 1, fieldName.size()));
    }

    switch (kafkaSourceType->getInputFormat()->getValue()) {
        case Configurations::InputFormat::JSON:
            inputParser = std::make_unique<JSONParser>(schema->getSize(), schemaKeys, physicalTypes);
            break;
        default: break;
    }
}

KafkaSource::~KafkaSource() {
    NES_INFO("Kafka source {} partition/group={} produced={} batchSize={} successFullPollCnt={} failedFullPollCnt={}",
             topic,
             groupId,
             bufferProducedCnt,
             batchSize,
             successFullPollCnt,
             failedFullPollCnt);
}

std::optional<Runtime::TupleBuffer> KafkaSource::receiveData() {
    NES_DEBUG("Kafka Source receiveData.");
    if (!connect()) {
        return std::nullopt;
    }
    auto tupleBuffer = allocateBuffer();
    try {
        do {
            if (!running) {
                return std::nullopt;
            }
            fillBuffer(tupleBuffer);
        } while (tupleBuffer.getNumberOfTuples() == 0);
    } catch (const std::exception& e) {
        NES_ERROR("KafkaSource::receiveData: Failed to fill the TupleBuffer. Error: {}.", e.what());
        throw e;
    }
    return tupleBuffer.getBuffer();
}

bool KafkaSource::fillBuffer(Runtime::MemoryLayouts::TestTupleBuffer& tupleBuffer) {

    const uint64_t tupleSize = schema->getSchemaSizeInBytes();
    const uint32_t tupleBufferCapacity = tupleBuffer.getCapacity();
    uint32_t tupleCount = 0;

    auto flushIntervalTimerStart = std::chrono::system_clock::now();
    bool flushIntervalPassed = false;

    while (tupleCount < tupleBufferCapacity && !flushIntervalPassed) {
        NES_DEBUG("KafkaSource tries to receive data...");
        //poll a batch of messages and put it into a vector
        messages = consumer->poll_batch(batchSize);
        consumer->async_commit();

        //iterate over the polled message buffer
        if (!messages.empty()) {

            NES_TRACE("KafkaSource poll {} ", messages.size());

            for (auto& message : messages) {
                if (tupleCount < tupleBufferCapacity) {

                    if (message.get_error()) {
                        if (!message.is_eof()) {
                            NES_ERROR("KafkaSource received error notification: {}", message.get_error().to_string());
                            throw message.get_error();
                        }
                        NES_WARNING("KafkaSource reached end of topic");
                        tupleBuffer.setNumberOfTuples(tupleCount);
                        return true;
                    }
                    inputParser->writeInputTupleToTupleBuffer(std::string(message.get_payload()),
                                                              tupleCount,
                                                              tupleBuffer,
                                                              schema,
                                                              localBufferManager);
                    tupleCount++;
                } else {
                    // FIXME: how to handle messages that are of size > tupleBufferCapacity
                    // NOTE: this will lead to missing tuples as we drop messages that do not fit in the buffer
                    NES_ERROR("KafkaSource polled messages do not fit into a single buffer");
                    tupleBuffer.setNumberOfTuples(tupleCount);
                    return true;
                }
            }
        }
        // If bufferFlushIntervalMs was defined by the user (> 0), we check whether the time on receiving
        // and writing data exceeds the user defined limit (bufferFlushIntervalMs).
        // If so, we flush the current TupleBuffer(TB) and proceed with the next TB.
        if ((bufferFlushIntervalMs > 0 && tupleCount > 0
             && std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - flushIntervalTimerStart)
                     .count()
                 >= bufferFlushIntervalMs)) {
            NES_DEBUG("KafkaSource::fillBuffer: Reached TupleBuffer flush interval. Finishing writing to current TupleBuffer.");
            flushIntervalPassed = true;
        }
    }
    tupleBuffer.setNumberOfTuples(tupleCount);
    return true;
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

        // Create the consumer
        consumer = std::make_unique<cppkafka::Consumer>(config);

        // Print the assigned partitions on assignment
        consumer->set_assignment_callback([](const cppkafka::TopicPartitionList& partitions) {
            // TODO do we want to keep this way as a work around for missing toString methods for cppkafka:: ?
            std::stringstream s;
            s << partitions;
            std::string partitionsAsString = s.str();
            NES_DEBUG("KafkaSource Got assigned {}", partitionsAsString);
        });

        // Print the revoked partitions on revocation
        consumer->set_revocation_callback([](const cppkafka::TopicPartitionList& partitions) {
            // TODO do we want to keep this way as a work around for missing toString methods for cppkafka:: ?
            std::stringstream s;
            s << partitions;
            std::string partitionsAsString = s.str();
            NES_DEBUG("KafkaSource Got revoked {}", partitionsAsString);
        });

        // Subscribe to the topic
        std::vector<cppkafka::TopicPartition> vec;
        cppkafka::TopicPartition assignment(topic, std::atoi(groupId.c_str()));
        vec.push_back(assignment);
        consumer->assign(vec);
        NES_DEBUG("kafka source={} connect to topic={} partition={}", this->operatorId, topic, std::atoi(groupId.c_str()));

        NES_DEBUG("kafka source starts producing");

        connected = true;
    }
    return connected;
}

SourceType KafkaSource::getType() const { return SourceType::KAFKA_SOURCE; }

std::string KafkaSource::getBrokers() const { return brokers; }

std::string KafkaSource::getTopic() const { return topic; }

std::string KafkaSource::getOffsetMode() const { return offsetMode; }

std::string KafkaSource::getGroupId() const { return groupId; }

uint64_t KafkaSource::getBatchSize() const { return batchSize; }

bool KafkaSource::isAutoCommit() const { return autoCommit; }

const std::chrono::milliseconds& KafkaSource::getKafkaConsumerTimeout() const { return kafkaConsumerTimeout; }

std::vector<PhysicalTypePtr> KafkaSource::getPhysicalTypes() const { return physicalTypes; }

const KafkaSourceTypePtr& KafkaSource::getSourceConfigPtr() const { return sourceConfig; }
}// namespace NES
#endif
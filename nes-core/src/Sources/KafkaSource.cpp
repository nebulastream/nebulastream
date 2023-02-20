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
#include <Runtime/BufferRecycler.hpp>
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
                         uint64_t numbersOfBufferToProduce,
                         const std::string brokers,
                         const std::string topic,
                         const std::string groupId,
                         bool autoCommit,
                         uint64_t kafkaConsumerTimeout,
                         std::string offsetMode,
                         const KafkaSourceTypePtr& sourceConfig,
                         OperatorId operatorId,
                         OriginId originId,
                         size_t numSourceLocalBuffers,
                         uint64_t batchSize,
                         const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors)
    : DataSource(schema,
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
    config->set("enable.auto.commit", autoCommit ? "true" : "false");
    config->set("auto.offset.reset", offsetMode);

    this->numBuffersToProcess = numbersOfBufferToProduce;

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

    switch (sourceConfig->getInputFormat()->getValue()) {
        case Configurations::InputFormat::JSON:
            inputParser = std::make_unique<JSONParser>(schema->getSize(), schemaKeys, physicalTypes);
            useJson = true;
            break;
        default:
            break;
    }
}

KafkaSource::~KafkaSource() {
    NES_INFO("Kafka source " << topic << " partition/group=" << groupId << " produced=" << bufferProducedCnt
              << " batchSize=" << batchSize << " successFullPollCnt=" << successFullPollCnt
              << " failedFullPollCnt=" << failedFullPollCnt << "reuseCnt=" << reuseCnt);
}

std::optional<Runtime::TupleBuffer> KafkaSource::receiveData() {
    NES_DEBUG("TCPSource  " << this << ": receiveData ");
    NES_DEBUG("TCPSource buffer allocated ");
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
        NES_ERROR("KafkaSource::receiveData: Failed to fill the TupleBuffer. Error: " << e.what());
        throw e;
    }
    return tupleBuffer.getBuffer();
}


bool KafkaSource::fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer) {
    uint64_t currentPollCnt = 0;
    NES_DEBUG("KAFKASOURCE tries to receive data...");
    //poll a batch of messages and put it into a vector
    messages = consumer->poll_batch(batchSize);

    //iterate over the polled message buffer
    if (!messages.empty()) {
        reuseCnt++;
        if (messages.back().get_error()) {
            if (!messages.back().is_eof()) {
                NES_WARNING("KAFKASOURCE received error notification: " << messages.back().get_error());
            }
            return false;
        } else {
            const uint64_t tupleSize = schema->getSchemaSizeInBytes();
            const uint64_t tupleCount = messages.back().get_payload().get_size() / tupleSize;
            const uint64_t payloadSize = messages.back().get_payload().get_size();

            NES_TRACE("KAFKASOURCE recv #tups: " << tupleCount << ", tupleSize: " << tupleSize << " payloadSize=" << payloadSize
                                                 << ", msg: " << messages.back().get_payload());
            auto currentTime = std::chrono::high_resolution_clock::now().time_since_epoch();
            auto timeStamp = std::chrono::duration_cast<std::chrono::milliseconds>(currentTime).count();

            for (auto& message : messages) {
                inputParser->writeInputTupleToTupleBuffer(std::string(message.get_payload()),
                                                          tupleCount, tupleBuffer,
                                                          schema, localBufferManager);
            }
            tupleBuffer.setNumberOfTuples(tupleCount);
            generatedTuples += tupleCount;
            messages.pop_back();
            generatedBuffers++;
            return true;
        }//end of else
    }
    NES_DEBUG("Poll NOT successfull for cnt=" << currentPollCnt++);
    failedFullPollCnt++;
    return false;
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

std::vector<PhysicalTypePtr> KafkaSource::getPhysicalTypes() const { return physicalTypes; }

const KafkaSourceTypePtr& KafkaSource::getSourceConfigPtr() const { return sourceConfig; }
}// namespace NES
#endif
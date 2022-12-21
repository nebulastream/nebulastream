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

#ifndef NES_CORE_INCLUDE_SOURCES_KAFKASOURCE_HPP_
#define NES_CORE_INCLUDE_SOURCES_KAFKASOURCE_HPP_

#ifdef ENABLE_KAFKA_BUILD
#include <cstdint>
#include <memory>
#include <string>
namespace cppkafka {
class Configuration;
class Consumer;
class Message;
}// namespace cppkafka

namespace NES {

class KafkaSource : public DataSource {
  public:
    KafkaSource(SchemaPtr schema,
                Runtime::BufferManagerPtr bufferManager,
                Runtime::QueryManagerPtr queryManager,
                uint64_t numbersOfBufferToProduce,
                std::string brokers,
                std::string topic,
                std::string groupId,
                bool autoCommit,
                uint64_t kafkaConsumerTimeout,
                std::string offsetMode,
                OriginId originId,
                OperatorId operatorId,
                size_t numSourceLocalBuffers,
                uint64_t batchSize,
                const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

    /**
     * @brief Get source type
     */
    SourceType getType() const override;
    ~KafkaSource() override;
    std::optional<Runtime::TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method for the kafka source
     * @return returns string describing the kafka source
     */
    std::string toString() const override;

    /**
     * @brief Get kafka brokers
     */
    std::string getBrokers() const;

    /**
     * @brief Get kafka topic
     */
    std::string getTopic() const;

    /**
     * @brief Get kafka offset
     */
    std::string getOffsetMode() const;

    /**
     * @brief Get kafka group id
     */
    std::string getGroupId() const;

    /**
     * @brief Get kafka batch size
     */
    uint64_t getBatchSize() const;

    /**
     * @brief If kafka offset is to be committed automatically
     */
    bool isAutoCommit() const;

    /**
     * @brief Get kafka connection timeout
     */
    const std::chrono::milliseconds& getKafkaConsumerTimeout() const;

  private:
    /**
     * @brief method to connect kafka using the host and port specified before
     * check if already connected, if not connect try to connect, if already connected return
     * @return bool indicating if connection could be established
     */
    bool connect();

    std::string brokers;
    std::string topic;
    std::string groupId;
    bool autoCommit;
    std::unique_ptr<cppkafka::Configuration> config;
    bool connected{false};
    std::chrono::milliseconds kafkaConsumerTimeout;
    std::string offsetMode;
    std::unique_ptr<cppkafka::Consumer> consumer;
    uint64_t bufferProducedCnt = 0;
    uint64_t batchSize = 1;
    uint64_t numberOfTuplesPerBuffer = 1;
    std::vector<cppkafka::Message> messages;
    uint64_t successFullPollCnt = 0;
    uint64_t failedFullPollCnt = 0;
    uint64_t reuseCnt = 0;
};

typedef std::shared_ptr<KafkaSource> KafkaSourcePtr;
}// namespace NES
#endif// NES_CORE_INCLUDE_SOURCES_KAFKASOURCE_HPP_
#endif
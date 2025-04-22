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
#ifndef NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_KAFKASINK_HPP_
#define NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_KAFKASINK_HPP_

#ifdef ENABLE_KAFKA_BUILD
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include <Sinks/Mediums/SinkMedium.hpp>

namespace cppkafka {
class Configuration;
class Producer;
class MessageBuilder;
}// namespace cppkafka
namespace NES {

class KafkaSink : public SinkMedium {
    constexpr static uint64_t INVALID_PARTITION_NUMBER = -1;

  public:
    /**
    * Constructor for a kafka Sink
    * @param format format of the sink
    * @param nodeEngine
    * @param numOfProducers
    * @param brokers list of brokers to connect to
    * @param topic list of topics to push to
    * @param sharedQueryId
    * @param decomposedQueryId
    * @param kafkaProducerTimeout timeout how long to wait until the push fails
    * @param numberOfOrigins
    * @param faultToleranceType fault tolerance type
    * @param checkpointStorageType checkpoint storage type
    */
    KafkaSink(SinkFormatPtr format,
              Runtime::NodeEnginePtr nodeEngine,
              uint32_t numOfProducers,
              const std::string& brokers,
              const std::string& topic,
              SharedQueryId sharedQueryId,
              DecomposedQueryId decomposedQueryId,
              DecomposedQueryPlanVersion decomposedQueryVersion,
              const uint64_t kafkaProducerTimeout = 10 * 1000,
              uint64_t numberOfOrigins = 1,
              FaultToleranceType faultToleranceType = FaultToleranceType::NONE,
              CheckpointStorageType checkpointStorageType = CheckpointStorageType::NONE,);

    ~KafkaSink() override;

    /**
     * @brief Get sink type
     */
    SinkMediumTypes getSinkMediumType() override;

    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) override;

    void setup() override;
    void shutdown() override;

    /**
     * @brief Get broker list
     */
    std::string getBrokers() const;

    /**
     * @brief Get kafka topic name
     */
    std::string getTopic() const;

    /**
     * @brief Get kafka producer timeout
     */
    uint64_t getKafkaProducerTimeout() const;
    std::string toString() const override;

  private:
    void connect();

    std::string brokers;
    std::string topic;

    std::unique_ptr<cppkafka::Configuration> config;
    std::unique_ptr<cppkafka::Producer> producer;
    std::unique_ptr<cppkafka::MessageBuilder> msgBuilder;

    std::chrono::milliseconds kafkaProducerTimeout;
};
using KafkaSinkPtr = std::shared_ptr<KafkaSink>;

}// namespace NES
#endif// ENABLE_KAFKA_BUILD
#endif// NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_KAFKASINK_HPP_

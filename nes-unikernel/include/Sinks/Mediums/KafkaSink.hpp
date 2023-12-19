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
#ifndef NES_CORE_INCLUDE_SINKS_MEDIUMS_KAFKASINK_HPP_
#define NES_CORE_INCLUDE_SINKS_MEDIUMS_KAFKASINK_HPP_
#include <Sinks/Formats/CsvFormat.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <chrono>
#include <cppkafka/cppkafka.h>
#include <cstdint>
#include <memory>
#include <string>

namespace cppkafka {
class Configuration;
class Producer;
class MessageBuilder;
}// namespace cppkafka
namespace NES {

template<typename Schema>
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
    * @param queryId
    * @param querySubPlanId
    * @param kafkaProducerTimeout timeout how long to wait until the push fails
    * @param faultToleranceType
    * @param numberOfOrigins
    */
    KafkaSink(uint32_t numOfProducers,
              const std::string& brokers,
              const std::string& topic,
              QueryId queryId,
              QuerySubPlanId querySubPlanId,
              const uint64_t kafkaProducerTimeout = 10 * 1000,
              FaultToleranceType faultToleranceType = FaultToleranceType::NONE,
              uint64_t numberOfOrigins = 1)
        : SinkMedium(numOfProducers, queryId, querySubPlanId, faultToleranceType, numberOfOrigins), brokers(brokers),
          topic(topic), kafkaProducerTimeout(std::chrono::milliseconds(kafkaProducerTimeout)) {

        config = std::make_unique<cppkafka::Configuration>();
        config->set("metadata.broker.list", brokers.c_str());

        connect();
        NES_DEBUG("KAFKASINK: Init KAFKA SINK to brokers  {} , topic  {}", brokers, topic);
    }

    ~KafkaSink() override = default;

    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) override {
        NES_TRACE("KAFKASINK: writes buffer");
        try {
            std::stringstream outputStream;
            NES_TRACE("KafkaSink::getData: write data");
            auto buffer = NES::Unikernel::printTupleBufferAsCSV<Schema, 8192>(inputBuffer);
            NES_TRACE("KafkaSink::getData: write buffer of size {}", buffer.size());
            cppkafka::Buffer kafkaBuffer(buffer.data(), buffer.size());
            msgBuilder->payload(kafkaBuffer);
            producer->produce(*msgBuilder);
            producer->flush(std::chrono::milliseconds(kafkaProducerTimeout));

            NES_DEBUG("KAFKASINK: send successfully");
        } catch (const cppkafka::HandleException& ex) {
            throw;
        } catch (...) {
            NES_ERROR("KAFKASINK Unknown error occurs");
            return false;
        }

        return true;
    }

    void setup() override{/*No Op*/};
    void shutdown() override{/*No Op*/};

    std::string toString() const override { return "KafkaSink"; };
    SinkMediumTypes getSinkMediumType() override { return SinkMediumTypes::KAFKA_SINK; };

  private:
    void connect() {
        NES_DEBUG("KAFKASINK connecting...");
        producer = std::make_unique<cppkafka::Producer>(*config);
        msgBuilder = std::make_unique<cppkafka::MessageBuilder>(topic);
    }

    std::string brokers;
    std::string topic;

    std::unique_ptr<cppkafka::Configuration> config;
    std::unique_ptr<cppkafka::Producer> producer;
    std::unique_ptr<cppkafka::MessageBuilder> msgBuilder;

    std::chrono::milliseconds kafkaProducerTimeout;
};

}// namespace NES
#endif// NES_INCLUDE_SINKS_MEDIUMS_KAFKASINK_HPP_
#endif// NES_CORE_INCLUDE_SINKS_MEDIUMS_KAFKASINK_HPP_

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
#include <Runtime/QueryManager.hpp>
#include <Sinks/Mediums/KafkaSink.hpp>
#include <Util/Logger/Logger.hpp>
#include <chrono>
#include <cppkafka/cppkafka.h>
#include <sstream>
#include <string>

using namespace std::chrono_literals;

namespace NES {

KafkaSink::KafkaSink(SinkFormatPtr format,
                     Runtime::NodeEnginePtr nodeEngine,
                     uint32_t numOfProducers,
                     const std::string& brokers,
                     const std::string& topic,
                     SharedQueryId sharedQueryId,
                     DecomposedQueryId decomposedQueryId,
                     DecomposedQueryPlanVersion decomposedQueryVersion,
                     const uint64_t kafkaProducerTimeout,
                     uint64_t numberOfOrigins,
                     FaultToleranceType faultToleranceType)
    : SinkMedium(format,
                 std::move(nodeEngine),
                 numOfProducers,
                 sharedQueryId,
                 decomposedQueryId,
                 decomposedQueryVersion,
                 faultToleranceType,
                 numberOfOrigins),
      brokers(brokers), topic(topic), kafkaProducerTimeout(std::chrono::milliseconds(kafkaProducerTimeout)) {

    config = std::make_unique<cppkafka::Configuration>();
    config->set("metadata.broker.list", brokers.c_str());

    connect();
    NES_DEBUG("KAFKASINK: Init KAFKA SINK to brokers  {} , topic  {}", brokers, topic);
}

KafkaSink::~KafkaSink() {}

bool KafkaSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) {
    NES_TRACE("KAFKASINK: writes buffer");
    try {
        std::stringstream outputStream;
        NES_TRACE("KafkaSink::getData: write data");
        auto buffer = sinkFormat->getFormattedBuffer(inputBuffer);
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

std::string KafkaSink::toString() const {
    std::stringstream ss;
    ss << "KAFKA_SINK(";
    ss << "BROKER(" << brokers << "), ";
    ss << "TOPIC(" << topic << ").";
    return ss.str();
}

void KafkaSink::setup() {
    // currently not required
}

void KafkaSink::shutdown() {
    // currently not required
}

void KafkaSink::connect() {
    NES_DEBUG("KAFKASINK connecting...");
    producer = std::make_unique<cppkafka::Producer>(*config);
    msgBuilder = std::make_unique<cppkafka::MessageBuilder>(topic);
    // FIXME: should we provide user to access partition ?
    // if (partition != INVALID_PARTITION_NUMBER) {
    // msgBuilder->partition(partition);
    // }
}

std::string KafkaSink::getBrokers() const { return brokers; }
std::string KafkaSink::getTopic() const { return topic; }
uint64_t KafkaSink::getKafkaProducerTimeout() const { return kafkaProducerTimeout.count(); }
SinkMediumTypes KafkaSink::getSinkMediumType() { return SinkMediumTypes::KAFKA_SINK; }

}// namespace NES
#endif

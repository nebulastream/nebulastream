/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <NodeEngine/QueryManager.hpp>
#include <Sources/KafkaSink.hpp>
#include <Util/Logger.hpp>
#include <chrono>
#include <sstream>
#include <string>
using namespace std::chrono_literals;

namespace NES {

KafkaSink::KafkaSink() {}

KafkaSink::KafkaSink(SchemaPtr schema,
                     const std::string& brokers,
                     const std::string& topic,
                     const size_t kafkaProducerTimeout)
    : DataSink(schema),
      brokers(brokers),
      topic(topic),
      kafkaProducerTimeout(std::move(std::chrono::milliseconds(kafkaProducerTimeout))) {

    config = {
        {"metadata.broker.list", brokers.c_str()}};

    _connect();
    NES_DEBUG("KAFKASINK  " << this << ": Init KAFKA SINK to brokers " << brokers
                            << ", topic " << topic << ", partition " << partition);
}

KafkaSink::~KafkaSink() {}

bool KafkaSink::writeData(TupleBuffer& input_buffer) {
    NES_DEBUG("KAFKASINK " << this << ": writes buffer " << input_buffer);
    try {
        cppkafka::Buffer buffer(input_buffer.getBuffer(),
                                input_buffer.getBufferSize());
        msgBuilder->payload(buffer);

        NES_DEBUG("KAFKASINK buffer to send " << buffer.get_size() << " bytes, content: " << msgBuilder->payload());
        producer->produce(*msgBuilder);
        producer->flush(kafkaProducerTimeout);
        NES_DEBUG("KAFKASINK " << this << ": send successfully");
    } catch (const cppkafka::HandleException& ex) {
        throw ex;
    } catch (...) {
        NES_ERROR("KAFKASINK Unknown error occurs");
        return false;
    }

    return true;
}

const std::string KafkaSink::toString() const {
    std::stringstream ss;
    ss << "KAFKA_SINK(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    ss << "BROKER(" << brokers << "), ";
    ss << "TOPIC(" << topic << "), ";
    ss << "PARTITION(" << partition << ").";
    return ss.str();
}

void KafkaSink::setup() {
    // currently not required
}

void KafkaSink::shutdown() {
    // currently not required
}

void KafkaSink::_connect() {

    NES_DEBUG("KAFKASINK connecting...");
    producer = std::make_unique<cppkafka::Producer>(config);
    msgBuilder = std::make_unique<cppkafka::MessageBuilder>(topic);
    // FIXME: should we provide user to access partition ?
    // if (partition != INVALID_PARTITION_NUMBER) {
    // msgBuilder->partition(partition);
    // }
}

SinkType KafkaSink::getType() const {
    return KAFKA_SINK;
}
const std::string KafkaSink::getBrokers() const {
    return brokers;
}
const std::string KafkaSink::getTopic() const {
    return topic;
}
const uint64_t KafkaSink::getKafkaProducerTimeout() const {
    return kafkaProducerTimeout.count();
}

}// namespace NES
#endif
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

#ifndef KAFKASINK_HPP
#define KAFKASINK_HPP
#ifdef ENABLE_KAFKA_BUILD
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include <Sources/SinkMedium.hpp>
#include <cppkafka/cppkafka.h>

namespace NES {

class KafkaSink : public SinkMedium {
    constexpr static uint64_t INVALID_PARTITION_NUMBER = -1;

  public:
    KafkaSink();
    KafkaSink(SchemaPtr schema, const std::string& brokers, const std::string& topic,
              const uint64_t kafkaProducerTimeout = 10 * 1000);

    ~KafkaSink() override;

    /**
     * @brief Get sink type
     */
    SinkMedium getType() const override;
    bool writeData(TupleBuffer& input_buffer);
    void setup() override;
    void shutdown() override;

    /**
     * @brief Get broker list
     */
    const std::string getBrokers() const;

    /**
     * @brief Get kafka topic name
     */
    const std::string getTopic() const;

    /**
     * @brief Get kafka producer timeout
     */
    const uint64_t getKafkaProducerTimeout() const;
    const std::string toString() const override;

  private:
    void _connect();

    std::string brokers;
    std::string topic;
    int partition;

    cppkafka::Configuration config;

    std::unique_ptr<cppkafka::Producer> producer;
    std::unique_ptr<cppkafka::MessageBuilder> msgBuilder;

    std::chrono::milliseconds kafkaProducerTimeout;
};
typedef std::shared_ptr<KafkaSink> KafkaSinkPtr;
}// namespace NES
#endif
#endif// KAFKASINK_HPP

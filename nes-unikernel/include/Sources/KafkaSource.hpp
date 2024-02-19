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
#include "DataSource.hpp"

#ifdef ENABLE_KAFKA_BUILD
#include <Operators/LogicalOperators/Sources/KafkaSourceDescriptor.hpp>
#include <Sinks/Formats/CsvFormat.hpp>
#include <Sources/Parsers/Parser.hpp>
#include <cppkafka/configuration.h>
#include <cppkafka/consumer.h>
#include <cstdint>
#include <memory>
#include <string>
namespace cppkafka {
class Consumer;
class Message;
}// namespace cppkafka

namespace NES {

template<typename T>
concept KafkaConfig = requires(T* t) {
    DataSourceConfig<T>;
    { T::Broker } -> std::same_as<const char* const&>;
    { T::Topic } -> std::same_as<const char* const&>;
    { T::GroupId } -> std::same_as<const int&>;
    { T::BatchSize } -> std::same_as<const size_t&>;
    { T::BufferFlushInterval } -> std::same_as<const std::chrono::milliseconds&>;
};

template<KafkaConfig Config>
class KafkaSource {
  public:
    using BufferType = NES::Unikernel::SchemaBuffer<typename Config::Schema, 8192>;
    constexpr static NES::SourceType SourceType = NES::SourceType::TCP_SOURCE;
    constexpr static bool NeedsBuffer = true;

    std::optional<Runtime::TupleBuffer> receiveData(const std::stop_token& stoken, BufferType schemaBuffer) {
        try {
            if (stoken.stop_requested()) {
                return std::nullopt;
            }
            if (!fillBuffer(schemaBuffer, stoken)) {
                return std::nullopt;
            }
        } catch (const std::exception& e) {
            NES_ERROR("TCPSource<Schema>::receiveData: Failed to fill the TupleBuffer. Error: {}.", e.what());
            throw e;
        }
        return schemaBuffer.getBuffer();
    }

    bool fillBuffer(BufferType& tupleBuffer, std::stop_token stoken) {
        using namespace std::chrono_literals;
        //init timer for flush interval
        auto flushIntervalTimerStart = std::chrono::system_clock::now();
        //init flush interval value
        bool flushIntervalPassed = false;
        while (!tupleBuffer.isFull() && !(flushIntervalPassed || stoken.stop_requested())) {
            auto messages = consumer->poll_batch(Config::BatchSize);
            for (const auto& message : messages) {
                if (message.get_error()) {
                    if (!message.is_eof()) {
                        NES_ERROR("KafkaSource received error notification: {}", message.get_error().to_string());
                        throw message.get_error();
                    }
                    NES_DEBUG("KafkaSource reached end of topic");
                    return false;
                }
                auto& payload = message.get_payload();
                auto dataSpan = std::span{reinterpret_cast<const char*>(payload.get_data()), payload.get_size()};
                Unikernel::parseCSVIntoBuffer(dataSpan, tupleBuffer);
            }

            if (!tupleBuffer.isEmpty() && Config::BufferFlushInterval > 0ms
                && std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()
                                                                         - flushIntervalTimerStart)
                    >= Config::BufferFlushInterval) {
                NES_DEBUG("TCPSource<Schema>::fillBuffer: Reached TupleBuffer flush interval. Finishing writing to current "
                          "TupleBuffer.");
                flushIntervalPassed = true;
            }
        }

        return true;
    }

    bool open() { return connect(); }
    bool close(Runtime::QueryTerminationType /*type*/) {
        NES_ASSERT(connected, "Calling close without ever establishing a connection");
        consumer.reset();
        return true;
    }

  private:
    /**
     * @brief method to connect kafka using the host and port specified before
     * check if already connected, if not connect try to connect, if already connected return
     * @return bool indicating if connection could be established
     */
    bool connect() {
        using namespace std::chrono_literals;
        if (!connected) {

            // Create the consumer
            consumer = std::make_unique<cppkafka::Consumer>(cppkafka::Configuration{{"metadata.broker.list", Config::Broker},
                                                                                    {"group.id", Config::GroupId},
                                                                                    {"auto.offset.reset", "earliest"},
                                                                                    // Disable auto commit
                                                                                    {"enable.auto.commit", false}});

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
            consumer->set_timeout(1s);
            consumer->set_log_level(cppkafka::LogLevel::LogInfo);
            consumer->subscribe({Config::Topic});
            NES_DEBUG("kafka source={} connect to topic={} partition={}", Config::OperatorId, Config::Topic, Config::GroupId);

            NES_DEBUG("kafka source starts producing");

            connected = true;
        }
        return connected;
    }
    bool connected{false};
    std::unique_ptr<cppkafka::Consumer> consumer;
};

}// namespace NES
#endif// NES_CORE_INCLUDE_SOURCES_KAFKASOURCE_HPP_
#endif// NES_CORE_INCLUDE_SOURCES_KAFKASOURCE_HPP_

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

#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_KAFKASOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_KAFKASOURCEDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical kafka source
 */
class KafkaSourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(SchemaPtr schema,
                                      std::string brokers,
                                      std::string topic,
                                      std::string groupId,
                                      bool autoCommit,
                                      uint64_t kafkaConnectTimeout, size_t sourceId);
    static SourceDescriptorPtr create(SchemaPtr schema,
                                      std::string streamName,
                                      std::string brokers,
                                      std::string topic,
                                      std::string groupId,
                                      bool autoCommit,
                                      uint64_t kafkaConnectTimeout, size_t sourceId);

    /**
     * @brief Get the list of kafka brokers
     */
    const std::string& getBrokers() const;

    /**
     * @brief Get the kafka topic name
     */
    const std::string& getTopic() const;

    /**
     * @brief Get the kafka consumer group id
     */
    const std::string& getGroupId() const;

    /**
     * @brief Is kafka topic offset to be auto committed
     */
    bool isAutoCommit() const;

    /**
     * @brief Get kafka connection timeout
     *
     * @return
     */
    uint64_t getKafkaConnectTimeout() const;
    bool equal(SourceDescriptorPtr other) override;
    std::string toString() override;

  private:
    explicit KafkaSourceDescriptor(SchemaPtr schema,
                                   std::string brokers,
                                   std::string topic,
                                   std::string groupId,
                                   bool autoCommit,
                                   uint64_t kafkaConnectTimeout, size_t sourceId);
    explicit KafkaSourceDescriptor(SchemaPtr schema,
                                   std::string streamName,
                                   std::string brokers,
                                   std::string topic,
                                   std::string groupId,
                                   bool autoCommit,
                                   uint64_t kafkaConnectTimeout, size_t sourceId);
    std::string brokers;
    std::string topic;
    std::string groupId;
    bool autoCommit;
    uint64_t kafkaConnectTimeout;
};

typedef std::shared_ptr<KafkaSourceDescriptor> KafkaSourceDescriptorPtr;

}// namespace NES

#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_KAFKASOURCEDESCRIPTOR_HPP_

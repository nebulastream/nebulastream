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

#ifndef NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_KAFKASOURCEDESCRIPTOR_HPP_
#define NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_KAFKASOURCEDESCRIPTOR_HPP_

#include <Catalogs/Source/PhysicalSourceTypes/KafkaSourceType.hpp>
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
                                      uint64_t kafkaConnectTimeout,
                                      std::string offsetMode,
                                      const KafkaSourceTypePtr& kafkaSourceType,
                                      uint64_t numbersOfBufferToProduce,
                                      uint64_t batchSize);
    static SourceDescriptorPtr create(SchemaPtr schema,
                                      std::string brokers,
                                      std::string logicalSourceName,
                                      std::string topic,
                                      std::string groupId,
                                      bool autoCommit,
                                      uint64_t kafkaConnectTimeout,
                                      std::string offsetMode,
                                      const KafkaSourceTypePtr& kafkaSourceType,
                                      uint64_t numbersOfBufferToProduce,
                                      uint64_t batchSize);

    static SourceDescriptorPtr create(SchemaPtr schema, KafkaSourceTypePtr sourceConfig);

    /**
     * @brief Get the list of kafka brokers
     */
    const std::string& getBrokers() const;

    /**
     * @brief Get the kafka topic name
     */
    const std::string& getTopic() const;

    /**
     * @brief Get the kafka offset mode
     */
    const std::string& getOffsetMode() const;

    /**
     * @brief Get the number of buffers to produce
     */
    std::uint64_t getNumberOfToProcessBuffers() const;

    /**
     * @brief Get the number of batches to pull from kafka in one call
     */
    std::uint64_t getBatchSize() const;

    /**
     * @brief Get the kafka consumer group id
     */
    const std::string& getGroupId() const;

    /**
     * @brief Get the input type for the source
     */
    KafkaSourceTypePtr getSourceConfigPtr() const;

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
    [[nodiscard]] bool equal(SourceDescriptorPtr const& other) const override;
    std::string toString() const override;

    SourceDescriptorPtr copy() override;

  private:
    explicit KafkaSourceDescriptor(SchemaPtr schema,
                                   std::string brokers,
                                   std::string topic,
                                   std::string groupId,
                                   bool autoCommit,
                                   uint64_t kafkaConnectTimeout,
                                   std::string offsetMode,
                                   const KafkaSourceTypePtr& kafkaSourceType,
                                   uint64_t numbersOfBufferToProduce,
                                   uint64_t batchSize);
    explicit KafkaSourceDescriptor(SchemaPtr schema,
                                   std::string logicalSourceName,
                                   std::string brokers,
                                   std::string topic,
                                   std::string groupId,
                                   bool autoCommit,
                                   uint64_t kafkaConnectTimeout,
                                   std::string offsetMode,
                                   const KafkaSourceTypePtr& kafkaSourceType,
                                   uint64_t numbersOfBufferToProduce,
                                   uint64_t batchSize);

    explicit KafkaSourceDescriptor(SchemaPtr schema, KafkaSourceTypePtr kafkaSourceType);

    std::string brokers;
    std::string topic;
    std::string groupId;
    KafkaSourceTypePtr kafkaSourceType;
    bool autoCommit;
    uint64_t kafkaConnectTimeout;
    std::string offsetMode;
    uint64_t numbersOfBufferToProduce;
    uint64_t batchSize;
};

using KafkaSourceDescriptorPtr = std::shared_ptr<KafkaSourceDescriptor>;

}// namespace NES

#endif// NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_KAFKASOURCEDESCRIPTOR_HPP_

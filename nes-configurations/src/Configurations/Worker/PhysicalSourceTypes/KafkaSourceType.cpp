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

#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/KafkaSourceType.hpp>
#include <Util/Logger/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

KafkaSourceTypePtr KafkaSourceType::create(const std::string& logicalSourceName, const std::string& physicalSourceName, Yaml::Node yamlConfig) {
    return std::make_shared<KafkaSourceType>(
        KafkaSourceType(logicalSourceName, physicalSourceName, std::move(yamlConfig)));
}

KafkaSourceTypePtr KafkaSourceType::create(const std::string& logicalSourceName,
                                           const std::string& physicalSourceName,
                                           std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<KafkaSourceType>(
        KafkaSourceType(logicalSourceName, physicalSourceName, std::move(sourceConfigMap)));
}

KafkaSourceTypePtr KafkaSourceType::create(const std::string& logicalSourceName, const std::string& physicalSourceName) {
    return std::make_shared<KafkaSourceType>(KafkaSourceType(logicalSourceName, physicalSourceName));
}

KafkaSourceType::KafkaSourceType(const std::string& logicalSourceName,
                                 const std::string& physicalSourceName,
                                 std::map<std::string, std::string> sourceConfigMap)
    : KafkaSourceType(logicalSourceName, physicalSourceName) {
    NES_INFO("KafkaSourceType: Init default Kafka source config object with values from command line args.");

    if (sourceConfigMap.find(Configurations::BROKERS_CONFIG) != sourceConfigMap.end()) {
        brokers->setValue(sourceConfigMap.find(Configurations::BROKERS_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("KafkaSourceConfig:: no brokers defined! Please define brokers.");
    }
    if (sourceConfigMap.find(Configurations::AUTO_COMMIT_CONFIG) != sourceConfigMap.end()) {
        autoCommit->setValue(std::stoi(sourceConfigMap.find(Configurations::AUTO_COMMIT_CONFIG)->second));
    }
    if (sourceConfigMap.find(Configurations::GROUP_ID_CONFIG) != sourceConfigMap.end()) {
        groupId->setValue(sourceConfigMap.find(Configurations::GROUP_ID_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("KafkaSourceConfig:: no groupId defined! Please define groupId.");
    }
    if (sourceConfigMap.find(Configurations::TOPIC_CONFIG) != sourceConfigMap.end()) {
        topic->setValue(sourceConfigMap.find(Configurations::TOPIC_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("KafkaSourceConfig:: no topic defined! Please define topic.");
    }
    if (sourceConfigMap.find(Configurations::OFFSET_MODE_CONFIG) != sourceConfigMap.end()) {
        offsetMode->setValue(sourceConfigMap.find(Configurations::OFFSET_MODE_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("KafkaSourceConfig:: no offsetMode defined! Please define offsetMode.");
    }
    if (sourceConfigMap.find(Configurations::CONNECTION_TIMEOUT_CONFIG) != sourceConfigMap.end()) {
        connectionTimeout->setValue(std::stoi(sourceConfigMap.find(Configurations::CONNECTION_TIMEOUT_CONFIG)->second));
    }
    if (sourceConfigMap.find(Configurations::NUMBER_OF_BUFFER_TO_PRODUCE) != sourceConfigMap.end()) {
        numberOfBuffersToProduce->setValue(std::stoi(sourceConfigMap.find(Configurations::NUMBER_OF_BUFFER_TO_PRODUCE)->second));
    }
    if (sourceConfigMap.find(Configurations::BATCH_SIZE) != sourceConfigMap.end()) {
        batchSize->setValue(std::stoi(sourceConfigMap.find(Configurations::BATCH_SIZE)->second));
    }
    if (sourceConfigMap.find(Configurations::INPUT_FORMAT_CONFIG) != sourceConfigMap.end()) {
        inputFormat->setInputFormatEnum(sourceConfigMap.find(Configurations::INPUT_FORMAT_CONFIG)->second);
    }
}

KafkaSourceType::KafkaSourceType(const std::string& logicalSourceName, const std::string& physicalSourceName, Yaml::Node yamlConfig)
    : KafkaSourceType(logicalSourceName, physicalSourceName) {
    NES_INFO("KafkaSourceType: Init default KAFKA source config object with values from YAML file.");

    if (!yamlConfig[Configurations::BROKERS_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::BROKERS_CONFIG].As<std::string>() != "\n") {
        brokers->setValue(yamlConfig[Configurations::BROKERS_CONFIG].As<std::string>());
    } else {
        NES_THROW_RUNTIME_ERROR("KafkaSourceType:: no brokers defined! Please define brokers.");
    }
    if (!yamlConfig[Configurations::AUTO_COMMIT_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::AUTO_COMMIT_CONFIG].As<std::string>() != "\n") {
        autoCommit->setValue(yamlConfig[Configurations::AUTO_COMMIT_CONFIG].As<uint32_t>());
    }
    if (!yamlConfig[Configurations::GROUP_ID_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::GROUP_ID_CONFIG].As<std::string>() != "\n") {
        groupId->setValue(yamlConfig[Configurations::GROUP_ID_CONFIG].As<std::string>());
    } else {
        NES_THROW_RUNTIME_ERROR("KafkaSourceType:: no groupId defined! Please define groupId.");
    }
    if (!yamlConfig[Configurations::TOPIC_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::TOPIC_CONFIG].As<std::string>() != "\n") {
        topic->setValue(yamlConfig[Configurations::TOPIC_CONFIG].As<std::string>());
    } else {
        NES_THROW_RUNTIME_ERROR("KafkaSourceType:: no topic defined! Please define topic.");
    }
    if (!yamlConfig[Configurations::OFFSET_MODE_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::OFFSET_MODE_CONFIG].As<std::string>() != "\n") {
        offsetMode->setValue(yamlConfig[Configurations::OFFSET_MODE_CONFIG].As<std::string>());
    } else {
        NES_THROW_RUNTIME_ERROR("KafkaSourceType:: no offset defined! Please define offset.");
    }
    if (!yamlConfig[Configurations::CONNECTION_TIMEOUT_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::CONNECTION_TIMEOUT_CONFIG].As<std::string>() != "\n") {
        connectionTimeout->setValue(yamlConfig[Configurations::CONNECTION_TIMEOUT_CONFIG].As<uint32_t>());
    }
    if (!yamlConfig[Configurations::NUMBER_OF_BUFFER_TO_PRODUCE].As<std::string>().empty()
        && yamlConfig[Configurations::NUMBER_OF_BUFFER_TO_PRODUCE].As<std::string>() != "\n") {
        numberOfBuffersToProduce->setValue(yamlConfig[Configurations::NUMBER_OF_BUFFER_TO_PRODUCE].As<uint32_t>());
    }
    if (!yamlConfig[Configurations::BATCH_SIZE].As<std::string>().empty()
        && yamlConfig[Configurations::BATCH_SIZE].As<std::string>() != "\n") {
        batchSize->setValue(yamlConfig[Configurations::BATCH_SIZE].As<uint32_t>());
    }
    if (!yamlConfig[Configurations::INPUT_FORMAT_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::INPUT_FORMAT_CONFIG].As<std::string>() != "\n") {
        inputFormat->setInputFormatEnum(yamlConfig[Configurations::INPUT_FORMAT_CONFIG].As<std::string>());
    }
}

KafkaSourceType::KafkaSourceType(const std::string& logicalSourceName, const std::string& physicalSourceName)
    : PhysicalSourceType(logicalSourceName, physicalSourceName, SourceType::KAFKA_SOURCE),
      brokers(Configurations::ConfigurationOption<std::string>::create(Configurations::BROKERS_CONFIG, "", "brokers")),

      autoCommit(Configurations::ConfigurationOption<uint32_t>::create(
          Configurations::AUTO_COMMIT_CONFIG,
          1,
          "auto commit, boolean value where 1 equals true, and 0 equals false, needed for: KafkaSource")),

      groupId(Configurations::ConfigurationOption<std::string>::create(
          Configurations::GROUP_ID_CONFIG,
          "testGroup",
          "userName, needed for: MQTTSource (can be chosen arbitrary), OPCSource")),

      topic(Configurations::ConfigurationOption<std::string>::create(Configurations::TOPIC_CONFIG,
                                                                     "testTopic",
                                                                     "topic to listen to")),
      offsetMode(Configurations::ConfigurationOption<std::string>::create(Configurations::OFFSET_MODE_CONFIG,
                                                                          "earliest",
                                                                          "Reading mode default earliest")),
      connectionTimeout(
          Configurations::ConfigurationOption<uint32_t>::create(Configurations::CONNECTION_TIMEOUT_CONFIG,
                                                                10,
                                                                "connection time out for source, needed for: KafkaSource")),
      numberOfBuffersToProduce(
          Configurations::ConfigurationOption<uint32_t>::create(Configurations::NUMBER_OF_BUFFER_TO_PRODUCE,
                                                                1,
                                                                "Numbers of events pulled from the queue overall")),

      batchSize(
          Configurations::ConfigurationOption<uint32_t>::create(Configurations::BATCH_SIZE,
                                                                1,
                                                                "Numbers of events pulled from the queue per pull request")),
      inputFormat(Configurations::ConfigurationOption<Configurations::InputFormat>::create(Configurations::INPUT_FORMAT_CONFIG,
                                                                                           Configurations::InputFormat::JSON,
                                                                                           "input data format")) {
    NES_INFO("KafkaSourceType: Init source config object with default values.");
}

std::string KafkaSourceType::toString() {
    std::stringstream ss;
    ss << "KafkaSourceType =>  {\n";
    ss << Configurations::BROKERS_CONFIG + ":" + brokers->toStringNameCurrentValue();
    ss << Configurations::AUTO_COMMIT_CONFIG + ":" + autoCommit->toStringNameCurrentValue();
    ss << Configurations::GROUP_ID_CONFIG + ":" + groupId->toStringNameCurrentValue();
    ss << Configurations::TOPIC_CONFIG + ":" + topic->toStringNameCurrentValue();
    ss << Configurations::OFFSET_MODE_CONFIG + ":" + offsetMode->toStringNameCurrentValue();
    ss << Configurations::CONNECTION_TIMEOUT_CONFIG + ":" + connectionTimeout->toStringNameCurrentValue();
    ss << Configurations::NUMBER_OF_BUFFER_TO_PRODUCE + ":" + numberOfBuffersToProduce->toStringNameCurrentValue();
    ss << Configurations::BATCH_SIZE + ":" + batchSize->toStringNameCurrentValue();
    ss << Configurations::INPUT_FORMAT_CONFIG + ":" + inputFormat->toStringNameCurrentValueEnum();
    ss << "\n}";
    return ss.str();
}

bool KafkaSourceType::equal(const PhysicalSourceTypePtr& other) {
    if (!other->instanceOf<KafkaSourceType>()) {
        return false;
    }
    auto otherSourceConfig = other->as<KafkaSourceType>();
    return brokers->getValue() == otherSourceConfig->brokers->getValue()
        && autoCommit->getValue() == otherSourceConfig->autoCommit->getValue()
        && groupId->getValue() == otherSourceConfig->groupId->getValue()
        && topic->getValue() == otherSourceConfig->topic->getValue()
        && offsetMode->getValue() == otherSourceConfig->offsetMode->getValue()
        && connectionTimeout->getValue() == otherSourceConfig->connectionTimeout->getValue()
        && numberOfBuffersToProduce->getValue() == otherSourceConfig->numberOfBuffersToProduce->getValue()
        && batchSize->getValue() == otherSourceConfig->batchSize->getValue()
        && inputFormat->getValue() == otherSourceConfig->inputFormat->getValue();
}

Configurations::StringConfigOption KafkaSourceType::getBrokers() const { return brokers; }

Configurations::IntConfigOption KafkaSourceType::getAutoCommit() const { return autoCommit; }

Configurations::StringConfigOption KafkaSourceType::getGroupId() const { return groupId; }

Configurations::StringConfigOption KafkaSourceType::getTopic() const { return topic; }

Configurations::StringConfigOption KafkaSourceType::getOffsetMode() const { return offsetMode; }

Configurations::InputFormatConfigOption KafkaSourceType::getInputFormat() const { return inputFormat; }

Configurations::IntConfigOption KafkaSourceType::getConnectionTimeout() const { return connectionTimeout; }

Configurations::IntConfigOption KafkaSourceType::getNumberOfBuffersToProduce() const { return numberOfBuffersToProduce; }

Configurations::IntConfigOption KafkaSourceType::getBatchSize() const { return batchSize; }

void KafkaSourceType::setBrokers(std::string brokersValue) { brokers->setValue(std::move(brokersValue)); }

void KafkaSourceType::setAutoCommit(uint32_t autoCommitValue) { autoCommit->setValue(autoCommitValue); }

void KafkaSourceType::setGroupId(std::string groupIdValue) { groupId->setValue(std::move(groupIdValue)); }

void KafkaSourceType::setTopic(std::string topicValue) { topic->setValue(std::move(topicValue)); }

void KafkaSourceType::setOffsetMode(std::string offsetModeValue) { offsetMode->setValue(std::move(offsetModeValue)); }

void KafkaSourceType::setConnectionTimeout(uint32_t connectionTimeoutValue) {
    connectionTimeout->setValue(connectionTimeoutValue);
}

void KafkaSourceType::setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduceValue) {
    numberOfBuffersToProduce->setValue(numberOfBuffersToProduceValue);
}

void KafkaSourceType::setBatchSize(uint32_t batchSizeValue) { batchSize->setValue(batchSizeValue); }

void KafkaSourceType::setInputFormat(std::string inputFormatValue) {
    inputFormat->setInputFormatEnum(std::move(inputFormatValue));
}

void KafkaSourceType::setInputFormat(Configurations::InputFormat inputFormatValue) {
    inputFormat->setValue(std::move(inputFormatValue));
}

uint64_t getBatchSize();

void KafkaSourceType::reset() {
    setBrokers(brokers->getDefaultValue());
    setAutoCommit(autoCommit->getDefaultValue());
    setGroupId(groupId->getDefaultValue());
    setTopic(topic->getDefaultValue());
    setOffsetMode(offsetMode->getDefaultValue());
    setConnectionTimeout(connectionTimeout->getDefaultValue());
    setNumberOfBuffersToProduce(numberOfBuffersToProduce->getDefaultValue());
    setBatchSize(batchSize->getDefaultValue());
    setInputFormat(inputFormat->getDefaultValue());
}
}// namespace NES
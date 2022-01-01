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

#include <Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>
#ifdef ENABLE_KAFKA_BUILD
namespace NES {

KafkaSinkDescriptor::KafkaSinkDescriptor(std::string topic, std::string brokers, uint64_t timeout)
    : topic(topic), brokers(brokers), timeout(timeout) {}

const std::string& KafkaSinkDescriptor::getTopic() const { return topic; }

const std::string& KafkaSinkDescriptor::getBrokers() const { return brokers; }

uint64_t KafkaSinkDescriptor::getTimeout() const { return timeout; }
SinkDescriptorPtr KafkaSinkDescriptor::create(std::string topic, std::string brokers, uint64_t timeout) {
    return std::make_shared<KafkaSinkDescriptor>(KafkaSinkDescriptor(topic, brokers, timeout));
}

}// namespace NES
#endif
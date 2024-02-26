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

#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/ThroughputSinkDescriptor.hpp>
#include <fmt/core.h>

NES::SinkDescriptorPtr NES::ThroughputSinkDescriptor::create(std::string counterName, size_t reportingThreshhold) {
    return std::shared_ptr<ThroughputSinkDescriptor>(new ThroughputSinkDescriptor(std::move(counterName), reportingThreshhold));
}
std::string NES::ThroughputSinkDescriptor::toString() const { return fmt::format("ThroughputSinkDescriptor({})", counterName); }
bool NES::ThroughputSinkDescriptor::equal(SinkDescriptorPtr const& other) {
    if (const auto otherThroughputSink = std::dynamic_pointer_cast<NES::ThroughputSinkDescriptor>(other)) {
        return otherThroughputSink->counterName == counterName;
    }
    return false;
}
const std::string& NES::ThroughputSinkDescriptor::getCounterName() const { return counterName; }
size_t NES::ThroughputSinkDescriptor::getReportingThreshhold() const { return reportingThreshhold; }
NES::ThroughputSinkDescriptor::ThroughputSinkDescriptor(std::string counterName, size_t reportingThreshhold)
    : counterName(std::move(counterName)), reportingThreshhold(reportingThreshhold) {}

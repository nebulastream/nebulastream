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

#include <Operators/LogicalOperators/Sinks/LatencySinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <fmt/core.h>

NES::SinkDescriptorPtr NES::LatencySinkDescriptor::create(size_t reportingThreshhold) {
    return std::shared_ptr<LatencySinkDescriptor>(new LatencySinkDescriptor(reportingThreshhold));
}
std::string NES::LatencySinkDescriptor::toString() const {
    return fmt::format("LatencySinkDescriptor({})", reportingThreshholdInMs);
}
bool NES::LatencySinkDescriptor::equal(SinkDescriptorPtr const& other) {
    if (const auto otherLatencySink = std::dynamic_pointer_cast<NES::LatencySinkDescriptor>(other)) {
        return otherLatencySink->reportingThreshholdInMs == reportingThreshholdInMs;
    }
    return false;
}
size_t NES::LatencySinkDescriptor::getReportingThreshhold() const { return reportingThreshholdInMs; }
NES::LatencySinkDescriptor::LatencySinkDescriptor(size_t reportingThreshhold) : reportingThreshholdInMs(reportingThreshhold) {}

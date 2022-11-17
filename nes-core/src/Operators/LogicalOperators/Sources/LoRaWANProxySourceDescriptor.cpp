// /*
//     Licensed under the Apache License, Version 2.0 (the "License");
//     you may not use this file except in compliance with the License.
//     You may obtain a copy of the License at
//         https://www.apache.org/licenses/LICENSE-2.0
//     Unless required by applicable law or agreed to in writing, software
//     distributed under the License is distributed on an "AS IS" BASIS,
//     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//     See the License for the specific language governing permissions and
//     limitations under the License.
// *

#include <Operators/LogicalOperators/Sources/LoRaWANProxySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <API/Schema.hpp>
#include <utility>

namespace NES {

LoRaWANProxySourceDescriptor::LoRaWANProxySourceDescriptor(SchemaPtr schema,
                                                           LoRaWANProxySourceTypePtr loRaWanProxySourceTypePtr,
                                                           std::string logicalSourceName,
                                                           std::string physicalSourceName)
    : SourceDescriptor(std::move(schema), std::move(logicalSourceName), std::move(physicalSourceName)),
      loRaWanProxySourceType(std::move(loRaWanProxySourceTypePtr)) {}

SourceDescriptorPtr LoRaWANProxySourceDescriptor::create(SchemaPtr schema,
                                                         LoRaWANProxySourceTypePtr sourceConfig,
                                                         std::string logicalSourceName,
                                                         std::string physicalSourceName) {
    return std::make_shared<LoRaWANProxySourceDescriptor>(
        LoRaWANProxySourceDescriptor(std::move(schema), std::move(sourceConfig), std::move(logicalSourceName), std::move(physicalSourceName)));
}
std::string LoRaWANProxySourceDescriptor::toString() {
    return "LoRaWANProxySourceDescriptor(" + loRaWanProxySourceType->toString() + ")";
}
bool LoRaWANProxySourceDescriptor::equal(const NES::SourceDescriptorPtr& other) {
    if (!other->instanceOf<LoRaWANProxySourceDescriptor>()) {
        return false;
    }
    auto otherConfig = other->as<LoRaWANProxySourceDescriptor>()->getSourceConfig();
    return otherConfig->equal(loRaWanProxySourceType);
}

SourceDescriptorPtr LoRaWANProxySourceDescriptor::copy() {
    return LoRaWANProxySourceDescriptor::create(schema->copy(),loRaWanProxySourceType, logicalSourceName, physicalSourceName); }

LoRaWANProxySourceTypePtr LoRaWANProxySourceDescriptor::getSourceConfig() const { return loRaWanProxySourceType; }

}// namespace NES

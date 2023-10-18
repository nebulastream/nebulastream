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
#include <Operators/LogicalOperators/Sources/NoOpSourceDescriptor.h>
#include <utility>
#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>

std::string NES::NoOpSourceDescriptor::toString() const {
    return "NoOpSourceDescriptor";
}

bool NES::NoOpSourceDescriptor::equal(const NES::SourceDescriptorPtr &other) const {
    return other->instanceOf<NoOpSourceDescriptor>() && other->getLogicalSourceName() == getLogicalSourceName();
}

NES::SourceDescriptorPtr NES::NoOpSourceDescriptor::copy() {
    return std::make_shared<NoOpSourceDescriptor>(schema, getLogicalSourceName());
}

NES::NoOpSourceDescriptor::NoOpSourceDescriptor(NES::SchemaPtr schemaPtr, std::string logicalSourceName)
        : SourceDescriptor(std::move(schemaPtr), std::move(logicalSourceName)) {
}

NES::SourceDescriptorPtr NES::NoOpSourceDescriptor::create(NES::SchemaPtr schemaPtr, std::string logicalSourceName) {
    return std::make_shared<NoOpSourceDescriptor>(std::move(schemaPtr), std::move(logicalSourceName));
}

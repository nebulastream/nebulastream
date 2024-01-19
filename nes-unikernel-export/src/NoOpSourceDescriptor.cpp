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
#include <NoOpSourceDescriptor.hpp>
#include <utility>

std::string NES::NoOpSourceDescriptor::toString() const { return "NoOpSourceDescriptor"; }

bool NES::NoOpSourceDescriptor::equal(const NES::SourceDescriptorPtr& other) const {
    return other->instanceOf<NoOpSourceDescriptor>() && other->getLogicalSourceName() == getLogicalSourceName()
        && other->getSchema() == getSchema() && other->as<NoOpSourceDescriptor>()->getTcp() == getTcp()
        && other->as<NoOpSourceDescriptor>()->getOperatorId() == getOperatorId()
        && other->as<NoOpSourceDescriptor>()->getOriginId() == getOriginId();
}

NES::SourceDescriptorPtr NES::NoOpSourceDescriptor::copy() {
    return std::make_shared<NoOpSourceDescriptor>(schema, schemaType, getLogicalSourceName(), tcp, originId, operatorId);
}

NES::NoOpSourceDescriptor::NoOpSourceDescriptor(SchemaPtr schema,
                                                SchemaType schemaType,
                                                std::string logicalSourceName,
                                                std::optional<TCPSourceConfiguration> tcp,
                                                NES::OriginId originId,
                                                NES::OperatorId operatorId)
    : SourceDescriptor(std::move(schema), std::move(logicalSourceName)), schemaType(schemaType), tcp(std::move(tcp)),
      originId(originId), operatorId(operatorId) {}

NES::SourceDescriptorPtr NES::NoOpSourceDescriptor::create(NES::SchemaPtr schemaPtr,
                                                           SchemaType schemaType,
                                                           std::string logicalSourceName,
                                                           std::optional<TCPSourceConfiguration> tcp,
                                                           OriginId originId,
                                                           OperatorId operatorId) {
    return std::make_shared<NoOpSourceDescriptor>(std::move(schemaPtr),
                                                  schemaType,
                                                  std::move(logicalSourceName),
                                                  std::move(tcp),
                                                  originId,
                                                  operatorId);
}

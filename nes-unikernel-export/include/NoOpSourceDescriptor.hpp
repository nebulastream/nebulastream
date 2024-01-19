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

#ifndef NES_NOOPSOURCEDESCRIPTOR_H
#define NES_NOOPSOURCEDESCRIPTOR_H

#include <CLIOptions.h>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {
class NoOpSourceDescriptor : public SourceDescriptor {
  public:
    NoOpSourceDescriptor(SchemaPtr schema,
                         SchemaType schemaType,
                         std::string logicalSourceName,
                         std::optional<TCPSourceConfiguration> tcp,
                         NES::OriginId originId,
                         NES::OperatorId operatorId);

    static SourceDescriptorPtr create(SchemaPtr schemaPtr,
                                      SchemaType schemaType,
                                      std::string logicalSourceName,
                                      std::optional<TCPSourceConfiguration>,
                                      OriginId originId,
                                      OperatorId operatorId);
    NoOpSourceDescriptor(SchemaPtr schemaPtr, std::string logicalSourceName, std::optional<TCPSourceConfiguration>);

    std::string toString() const override;

    bool equal(const SourceDescriptorPtr& other) const override;

    SourceDescriptorPtr copy() override;

    [[nodiscard]] NES::OriginId getOriginId() const { return originId; }

    [[nodiscard]] NES::OperatorId getOperatorId() const { return operatorId; }

    [[nodiscard]] std::optional<TCPSourceConfiguration> getTcp() const { return tcp; }
    [[nodiscard]] SchemaType getSchemaType() const { return schemaType; }

  private:
    SchemaType schemaType;
    std::optional<TCPSourceConfiguration> tcp;
    NES::OriginId originId;
    NES::OperatorId operatorId;
};
}// namespace NES
#endif//NES_NOOPSOURCEDESCRIPTOR_H

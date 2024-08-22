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

#pragma once

#include <Configurations/Worker/PhysicalSourceTypes/TCPSourceType.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES
{

class TCPSourceDescriptor : public SourceDescriptor
{
public:
    static std::unique_ptr<SourceDescriptor> create(SchemaPtr schema, TCPSourceTypePtr tcpSourceType, const std::string& logicalSourceName);

    static std::unique_ptr<SourceDescriptor> create(SchemaPtr schema, TCPSourceTypePtr sourceConfig);
    /**
     * @brief get TCP source config
     * @return tcp source config
     */
    TCPSourceTypePtr getSourceConfig() const;

    /**
     * checks if two tcp source descriptors are the same
     * @param other
     * @return true if they are the same
     */
    [[nodiscard]] bool equal(SourceDescriptor& other) const override;

    std::string toString() const override;

private:
    explicit TCPSourceDescriptor(SchemaPtr schema, TCPSourceTypePtr tcpSourceType, const std::string& logicalSourceName);

    TCPSourceTypePtr tcpSourceType;
};

} /// namespace NES

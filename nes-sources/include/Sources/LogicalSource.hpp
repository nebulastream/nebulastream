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

#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <string>

#include <Util/Logger/Formatter.hpp>
#include <DataTypes/UnboundSchema.hpp>

namespace NES
{
class SourceCatalog;
class OperatorSerializationUtil;

class LogicalSource
{
    friend SourceCatalog;
    friend OperatorSerializationUtil;
    explicit LogicalSource(std::string logicalSourceName, const UnboundSchema& schema);

public:
    [[nodiscard]] std::string getLogicalSourceName() const;

    [[nodiscard]] std::shared_ptr<const UnboundSchema> getSchema() const;
    friend std::ostream& operator<<(std::ostream& os, const LogicalSource& logicalSource);

    friend bool operator==(const LogicalSource& lhs, const LogicalSource& rhs);
    friend bool operator!=(const LogicalSource& lhs, const LogicalSource& rhs);

private:
    std::string logicalSourceName;
    /// Keep schemas in logical sources dynamically allocated to avoid unnecessary copies
    std::shared_ptr<const UnboundSchema> schema;
};

}

template <>
struct std::hash<NES::LogicalSource>
{
    uint64_t operator()(const NES::LogicalSource& logicalSource) const noexcept;
};

FMT_OSTREAM(NES::LogicalSource);

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

#include <Sources/LogicalSource.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <DataTypes/Schema.hpp>
#include <fmt/format.h>

namespace NES
{


LogicalSource::LogicalSource(std::string logicalSourceName, std::shared_ptr<const Schema>&& schema)
    : logicalSourceName(std::move(logicalSourceName)), schema(std::move(schema))
{
}

std::shared_ptr<const Schema> LogicalSource::getSchema() const
{
    return schema;
}

std::string LogicalSource::getLogicalSourceName() const
{
    return logicalSourceName;
}

bool operator==(const LogicalSource& lhs, const LogicalSource& rhs)
{
    return lhs.logicalSourceName == rhs.logicalSourceName && *lhs.schema == *rhs.schema;
}
bool operator!=(const LogicalSource& lhs, const LogicalSource& rhs)
{
    return !(lhs == rhs);
}
}
uint64_t std::hash<NES::LogicalSource>::operator()(const NES::LogicalSource& logicalSource) const noexcept
{
    return std::hash<std::string>()(logicalSource.getLogicalSourceName());
}
std::ostream& NES::operator<<(std::ostream& os, const LogicalSource& logicalSource)
{
    return os << fmt::format("LogicalSource(name: {}, schema{})", logicalSource.getLogicalSourceName(), *logicalSource.getSchema());
}

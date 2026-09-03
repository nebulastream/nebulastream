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

#include <cstddef>
#include <functional>
#include <string>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/Identifiers.hpp>

namespace NES
{

/// The names stay plain strings rather than Identifiers: a request is textual, it arrives from outside the
/// engine, and the generator is the one place that needs engine identifiers, so it parses them there.

/// Statistics collected over a logical source field.
struct DataDomain
{
    std::string logicalSourceName;
    std::string fieldName;
    bool operator==(const DataDomain&) const = default;

    [[nodiscard]] size_t hash() const { return std::hash<std::string>{}(logicalSourceName + "." + fieldName); }
};

/// Statistics collected over an operator's output in an existing query. Accepted but not implemented.
struct WorkloadDomain
{
    QueryId queryId;
    OperatorId operatorId;
    std::string fieldName;
    bool operator==(const WorkloadDomain&) const = default;

    [[nodiscard]] size_t hash() const
    {
        return std::hash<std::string>{}(
            std::to_string(queryId.getRawValue()) + "." + std::to_string(operatorId.getRawValue()) + "." + fieldName);
    }
};

/// Statistics collected for a worker node. Accepted but not implemented.
struct InfrastructureDomain
{
    Host hostId;
    bool operator==(const InfrastructureDomain&) const = default;

    [[nodiscard]] size_t hash() const { return std::hash<std::string>{}(hostId.getRawValue()); }
};

using CollectionDomain = std::variant<DataDomain, WorkloadDomain, InfrastructureDomain>;

}

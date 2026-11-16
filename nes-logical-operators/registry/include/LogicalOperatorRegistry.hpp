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

#include <expected>
#include <functional>
#include <string>
#include <vector>

#include <Operators/LogicalOperator.hpp>
#include <Types/NESType.hpp>
#include <Util/RuntimeRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

using LogicalOperatorRegistryReturnType = std::expected<LogicalOperator, Exception>;
using LogicalOperatorFn = std::function<LogicalOperatorRegistryReturnType(const NESValueParameters&)>;

struct LogicalOperatorTrait
{
};

/// Wraps a `LogicalOperator` into a `NESValue` carrying `LogicalOperatorTrait` (`LogicalOperator` type class).
[[nodiscard]] NESValue makeLogicalOperatorValue(LogicalOperator op);

/// Frontend-exposed registry for table-valued functions / relational operator constructors (e.g. `MODEL_INFERENCE`).
class LogicalOperatorRegistry : public RuntimeRegistry<LogicalOperatorRegistry, std::string, LogicalOperatorFn, /*CaseSensitive*/ false>
{
public:
    static constexpr bool EXPOSE_TO_SQL_SCOPE = true;

    static LogicalOperatorRegistry& instance();
    static NESTypeClass logicalOperatorTypeClass();
    static NamedNESParametrizedTypeClass getParametrizedTypeClass();
    static std::vector<NamedNESParametrizedTypeClass> getParametrizedTypeClasses();
};

}

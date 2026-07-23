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

#include <functional>
#include <string>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Util/Reflection.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

using LogicalOperatorRegistryReturnType = LogicalOperator;

struct LogicalOperatorRegistryArguments
{
    std::vector<LogicalOperator> children;
    Reflected reflected;
};

using LogicalOperatorFn = std::function<LogicalOperatorRegistryReturnType(LogicalOperatorRegistryArguments)>;

/// Name-based construction surface for logical operators. Currently has no entries: operators
/// gain one by implementing a static create member and adding an
/// add_registry_entry(LogicalOperator <name>) line.
class LogicalOperatorRegistry : public RuntimeRegistry<LogicalOperatorRegistry, std::string, LogicalOperatorFn, /*CaseSensitive*/ false>
{
public:
    static LogicalOperatorRegistry& instance();
};

}

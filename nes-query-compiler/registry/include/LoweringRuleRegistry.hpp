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
#include <memory>
#include <string>
#include <utility>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Util/RuntimeRegistry.hpp>
#include <QueryExecutionConfiguration.hpp>

namespace NES
{

using LoweringRuleRegistryReturnType = std::unique_ptr<AbstractLoweringRule>;

struct LoweringRuleRegistryArguments
{
    QueryExecutionConfiguration conf;
};

using LoweringRuleFn = std::function<LoweringRuleRegistryReturnType(LoweringRuleRegistryArguments)>;

/// Creates the registry entry for a lowering rule: rules are constructed from the query
/// execution configuration.
template <typename LoweringRuleImpl>
LoweringRuleFn makeLoweringRule()
{
    return [](LoweringRuleRegistryArguments arguments) -> LoweringRuleRegistryReturnType
    { return std::make_unique<LoweringRuleImpl>(arguments.conf); };
}

/// Filled by loadBuiltinPlugins() / plugin registration (see cmake/RuntimeRegistrationUtil.cmake).
/// Case-insensitive to mirror the retired BaseRegistry.
class LoweringRuleRegistry : public RuntimeRegistry<LoweringRuleRegistry, std::string, LoweringRuleFn, /*CaseSensitive*/ false>
{
public:
    /// Defined out-of-line (LoweringRuleRegistry.cpp) so exactly one instance exists
    /// process-wide even with plugins loaded as shared objects.
    static LoweringRuleRegistry& instance();
};

}

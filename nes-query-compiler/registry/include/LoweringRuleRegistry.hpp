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

#include <concepts>
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

class AbstractStatisticStore;

struct LoweringRuleRegistryArguments
{
    QueryExecutionConfiguration conf;
    /// The worker's statistic store; may be null. Only rules that declare a constructor taking the whole
    /// arguments struct receive it (see makeLoweringRule).
    std::shared_ptr<AbstractStatisticStore> statisticStore;
};

using LoweringRuleFn = std::function<LoweringRuleRegistryReturnType(LoweringRuleRegistryArguments)>;

/// Creates the registry entry for a lowering rule. Rules that only need the query execution configuration
/// declare a constructor taking QueryExecutionConfiguration; rules that additionally need e.g. the statistic
/// store declare a constructor taking the whole LoweringRuleRegistryArguments struct, which takes precedence.
/// NOTE: adding a converting constructor from LoweringRuleRegistryArguments to an existing rule silently
/// switches it to the arguments branch.
template <typename LoweringRuleImpl>
LoweringRuleFn makeLoweringRule()
{
    return [](LoweringRuleRegistryArguments arguments) -> LoweringRuleRegistryReturnType
    {
        if constexpr (std::constructible_from<LoweringRuleImpl, LoweringRuleRegistryArguments>)
        {
            return std::make_unique<LoweringRuleImpl>(std::move(arguments));
        }
        else
        {
            return std::make_unique<LoweringRuleImpl>(arguments.conf);
        }
    };
}

class LoweringRuleRegistry : public RuntimeRegistry<LoweringRuleRegistry, std::string, LoweringRuleFn, /*CaseSensitive*/ false>
{
public:
    static LoweringRuleRegistry& instance();
};

}

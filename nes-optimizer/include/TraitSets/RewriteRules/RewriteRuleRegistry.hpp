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

#include <TraitSets/RewriteRule/RewriteRule.hpp>
#include "Util/PluginRegistry.hpp"

namespace NES
{

class RegistryRewriteRule
{
public:

    /// Custom register function to support
    template <
        typename Rule,
        typename InputTraitSet,
        typename OutputTraitSet
        >
    requires RewriteRuleConcept<Rule, InputTraitSet, OutputTraitSet>
    void registerRule(const std::string& name)
    {
        if (registry.find(name) != registry.end()) {
            throw std::runtime_error("Rewrite rule with name '" + name + "' is already registered.");
        }

        std::function<OutputTraitSet(const InputTraitSet&)> func = [](const InputTraitSet& input) -> OutputTraitSet {
            return Rule::apply(input);
        };

        registry.emplace(
            name,
            RewriteRuleEntry{
                std::any(func),
                std::type_index(typeid(InputTraitSet)),
                std::type_index(typeid(OutputTraitSet))
            }
        );
    }

    template <typename InputTraitSet, typename OutputTraitSet>
    OutputTraitSet applyRule(const std::string& name, const InputTraitSet& input) const
    {
        auto it = registry.find(name);
        if (it == registry.end()) {
            throw std::runtime_error("Rewrite rule with name '" + name + "' not found.");
        }

        const RewriteRuleEntry& entry = it->second;

        if (entry.inputType != std::type_index(typeid(InputTraitSet)) ||
            entry.outputType != std::type_index(typeid(OutputTraitSet))) {
            throw std::runtime_error("TraitSet types do not match for rewrite rule '" + name + "'.");
        }

        try
        {
            auto func = std::any_cast<std::function<OutputTraitSet(const InputTraitSet&)>>(entry.func);
            return func(input);
        }
        catch (const std::bad_any_cast&)
        {
            throw std::runtime_error("Failed to cast the function for rewrite rule '" + name + "'.");
        }
    }

private:
    struct RewriteRuleEntry
    {
        std::any func; /// for std::function<OutputTraitSet(const InputTraitSet&)>
        std::type_index inputType;
        std::type_index outputType;
    };

    std::unordered_map<std::string, RewriteRuleEntry> registry;
};
}

/*
using RegistryRewriteRuleSignature = RegistrySignature<std::string, RewriteRule, std::vector<std::unique_ptr<RewriteRule>>>;
class RegistryRewriteRule : public BaseRegistry<RegistryRewriteRule, RegistryRewriteRuleSignature>
{
};
}*/

#define INCLUDED_FROM_REGISTRY_REWRITE_RULE
#include <TraitSets/RewriteRule/GeneratedRewriteRuleRegistrar.inc>
#undef INCLUDED_FROM_REGISTRY_REWRITE_RULE
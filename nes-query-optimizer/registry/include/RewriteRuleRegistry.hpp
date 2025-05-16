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

#include <memory>
#include <string>
#include <Configurations/Worker/QueryOptimizerConfiguration.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Util/Registry.hpp>

namespace NES
{

using RewriteRuleRegistryReturnType = std::unique_ptr<AbstractRewriteRule>;
struct RewriteRuleRegistryArguments
{
    NES::Configurations::QueryOptimizerConfiguration conf;
};

class RewriteRuleRegistry
    : public BaseRegistry<RewriteRuleRegistry, std::string, RewriteRuleRegistryReturnType, RewriteRuleRegistryArguments>
{
};
}

#define INCLUDED_FROM_REGISTRY_REWRITE_RULE
#include <RewriteRuleGeneratedRegistrar.inc>
#undef INCLUDED_FROM_REGISTRY_REWRITE_RULE

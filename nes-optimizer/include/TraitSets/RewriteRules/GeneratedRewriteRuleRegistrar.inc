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

#ifndef INCLUDED_FROM_REGISTRY_REWRITE_RULE
#    error "This file should not be included directly! " \
"Include instead include <TraitSets/RewriteRules/RewriteRuleRegistry.hpp>"
#endif


namespace NES::RewriteRuleGeneratedRegistrar
{
std::unique_ptr<NES::Optimizer::AbstractRewriteRule> RegisterLowerToPhysicalMap();
std::unique_ptr<NES::Optimizer::AbstractRewriteRule> RegisterLowerToPhysicalSelection();
std::unique_ptr<NES::Optimizer::AbstractRewriteRule> RegisterLowerToPhysicalJoin();
}

namespace NES
{
template <>
inline void Registrar<RewriteRuleRegistry, RewriteRuleRegistrySignature>::registerAll([[maybe_unused]] Registry<Registrar>& registry)
{
    using namespace NES::RewriteRuleGeneratedRegistrar;
    registry.registerPlugin("LowerToPhysicalMap", RegisterLowerToPhysicalMap);
    registry.registerPlugin("LowerToPhysicalSelection", RegisterLowerToPhysicalSelection);
    registry.registerPlugin("LowerToPhysicalJoin", RegisterLowerToPhysicalJoin);
}
}

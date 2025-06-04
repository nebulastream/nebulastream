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

#include <utility>
#include <Configurations/Worker/QueryOptimizerConfiguration.hpp>
#include <Operators/LogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>

namespace NES
{
struct LowerToPhysicalHashJoin : AbstractRewriteRule
{
    explicit LowerToPhysicalHashJoin(NES::Configurations::QueryOptimizerConfiguration conf) : conf(std::move(conf)) { }
    RewriteRuleResultSubgraph apply(LogicalOperator logicalOperator) override;

private:
    NES::Configurations::QueryOptimizerConfiguration conf;
};

}

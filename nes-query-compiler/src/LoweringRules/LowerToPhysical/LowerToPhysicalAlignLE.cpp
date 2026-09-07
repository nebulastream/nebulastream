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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalAlignLE.hpp>

#include <utility>

#include <LoweringRules/AbstractLoweringRule.hpp>
#include <LoweringRules/LowerToPhysical/LowerToPhysicalAlignDirectedCommon.hpp>
#include <Operators/LogicalOperator.hpp>
#include <LoweringRuleRegistry.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalAlignLE::apply(LogicalOperator logicalOperator)
{
    return lowerDirectedAlign(std::move(logicalOperator), conf.pageSize.getValue());
}
}

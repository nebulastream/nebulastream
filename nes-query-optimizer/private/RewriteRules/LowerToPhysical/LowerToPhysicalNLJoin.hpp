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

#include <cstdint>
#include <memory>
#include <tuple>
#include <utility>
#include <Traits/QueryForSubtree.hpp>
#include <Traits/TraitSet.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Configurations/Worker/QueryOptimizerConfiguration.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/Operator.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <Streaming/Join/NestedLoopJoin/NLJBuildPhysicalOperator.hpp>
#include <Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Streaming/Join/NestedLoopJoin/NLJProbePhysicalOperator.hpp>
#include <Streaming/Join/StreamJoinUtil.hpp>
#include <Util/Common.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>

namespace NES::Optimizer
{
struct LowerToPhysicalNLJoin : AbstractLowerToPhysicalRewriteRule<QueryForSubtree, Operator>
{
    LowerToPhysicalNLJoin(const NES::Configurations::QueryOptimizerConfiguration& conf) : conf(conf) {}
    std::vector<PhysicalOperatorWithSchema> applyToPhysical(DynamicTraitSet<QueryForSubtree, Operator>*) override;
    const NES::Configurations::QueryOptimizerConfiguration& conf;
};

}

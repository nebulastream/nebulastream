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
#include <set>
#include <TraitSets/TraitSet.hpp>
#include <TraitSets/Traits/Children.hpp>
#include <TraitSets/Traits/QueryForSubtree.hpp>
#include <TraitSets/RewriteRules/AbstractRewriteRule.hpp>
#include <Operators/LogicalOperators/Windows/Joins/JoinLogicalOperator.hpp>
#include <Operators/Operator.hpp>
#include <Streaming/Join/NestedLoopJoin/NLJProbe.hpp>
#include <Streaming/Join/NestedLoopJoin/NLJBuild.hpp>
#include <Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>

namespace NES::Optimizer
{

struct LowerToPhysicalNLJoin : TypedAbstractRewriteRule<QueryForSubtree, Operator>
{
    DynamicTraitSet<QueryForSubtree, Operator>* applyTyped(DynamicTraitSet<QueryForSubtree, Operator>* traitSet) override
    {
        auto ops = traitSet->get<JoinLogicalOperator>();
        auto outSchema = ops->getOutputSchema();

        /// TODO Where do we place the operator handler?
        /// TODO How do we get the memory provider?

        /*
        auto buildOp = std::make_shared<NLJBuild>(
            uint64_t operatorHandlerIndex,
            QueryCompilation::JoinBuildSideType joinBuildSide,
            std::unique_ptr<TimeFunction> timeFunction,
            const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& memoryProvider);
        */
        /*
        auto probeOp = std::make_shared<NLJProbe>(uint64_t operatorHandlerIndex,
                const std::shared_ptr<Functions::Function>& joinFunction,
                const WindowMetaData& windowMetaData,
                const JoinSchema& joinSchema,
                const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& leftMemoryProvider,
                const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& rightMemoryProvider);
        */
        return traitSet;
    };
};

}

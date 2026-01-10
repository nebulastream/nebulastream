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
#include <Phases/DecideMemoryLayout.hpp>

#include <ranges>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/MemorySwapLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <ErrorHandling.hpp>
#include <QueryExecutionConfiguration.hpp>

namespace NES
{
LogicalPlan DecideMemoryLayout::apply(const LogicalPlan& queryPlan, const QueryExecutionConfiguration& conf)
{
    PRECONDITION(queryPlan.getRootOperators().size() == 1, "Only single root operators are supported for now");
    PRECONDITION(not queryPlan.getRootOperators().empty(), "Query must have a sink root operator");
    return LogicalPlan{queryPlan.getQueryId(), {apply(queryPlan.getRootOperators()[0], conf)}};
}

LogicalOperator DecideMemoryLayout::apply(const LogicalOperator& logicalOperator, const QueryExecutionConfiguration& conf)
{
    /// Iterating over all operators and setting the memory layout trait to row
    auto traitSet = logicalOperator.getTraitSet();

    if (logicalOperator.getName() == "Source") /// add swap after source
    {
        /// source currently only in row layout
        tryInsert(traitSet, MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT});
    }
    else if (logicalOperator.getName() == "Sink")
    { /// add swap before sink
        /// sink currently only in row layout
        tryInsert(traitSet, MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT});
    }
    else
    {
        if (conf.layoutStrategy == MemoryLayoutStrategy::SWAP_ALL_COL)
        {
            tryInsert(traitSet, MemoryLayoutTypeTrait{MemoryLayoutType::COLUMNAR_LAYOUT});
        }
        else
        {
            tryInsert(traitSet, MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT});
        }
    }

    auto children = logicalOperator.getChildren()
        | std::views::transform([this, &conf](const LogicalOperator& child) { return apply(child, conf); })
        | std::ranges::to<std::vector>();

    /// insert memory swaps where needed
    if (conf.layoutStrategy == MemoryLayoutStrategy::SWAP_ALL_COL || conf.layoutStrategy == MemoryLayoutStrategy::SWAP_ALL_ROW)
    {
        if (logicalOperator.getName() == "Sink")
        {
            PRECONDITION(!children.empty(), "Sink operator must have at least one child");
            if (children[0].getName() != "Source")
            { /// no swap needed if empty query
                auto childLayoutType = children[0].getTraitSet().get<MemoryLayoutTypeTrait>().memoryLayout;
                auto memorySwap = MemorySwapLogicalOperator(children[0].getOutputSchema(), childLayoutType, MemoryLayoutType::ROW_LAYOUT);
                return logicalOperator.withChildren({memorySwap.withChildren(children)}).withTraitSet(traitSet);
            }
        }
        else if (!children.empty() && children[0].getName() == "Source")
        {
            ///insert memory swap before source
            auto operatorLayoutType = traitSet.get<MemoryLayoutTypeTrait>().memoryLayout;
            auto memorySwaps = std::vector<LogicalOperator>{};
            /// create one swap for each source
            for (const auto& child : children)
            {
                if (child.getName() != "Source")
                {
                    memorySwaps.push_back(child);
                    continue;
                }
                auto memorySwap = MemorySwapLogicalOperator(
                    child.getOutputSchema(),
                    MemoryLayoutType::ROW_LAYOUT,
                    operatorLayoutType,
                    child.getAs<SourceDescriptorLogicalOperator>()->getSourceDescriptor().getParserConfig());
                memorySwaps.emplace_back(memorySwap.withChildren({child}).withTraitSet(traitSet));
            }

            return logicalOperator.withChildren(memorySwaps).withTraitSet(traitSet);
        }
    }

    return logicalOperator.withChildren(children).withTraitSet(traitSet);
}
}

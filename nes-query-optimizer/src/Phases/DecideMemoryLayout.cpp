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
        /// row_layout_ratio likelihood of the layout being row
        std::random_device rd;
        std::mt19937 gen(rd());

        if (!traitSet.contains<MemoryLayoutTypeTrait>())
        { /// check if not already set
            if (std::uniform_real_distribution<> dis(0.0, 1.0); dis(gen) <= conf.rowLayoutRatio.getValue())
            {
                tryInsert(traitSet, MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT});
            }
            else
            {
                tryInsert(traitSet, MemoryLayoutTypeTrait{MemoryLayoutType::COLUMNAR_LAYOUT});
            }
        }
    }

    auto childs = logicalOperator.getChildren();

    if (conf.rowLayoutRatio.getValue() > 0 && conf.rowLayoutRatio.getValue() < 1.0)
    {
        if (logicalOperator.getName() == "Join" || logicalOperator.getName() == "WindowedAggregation")
        {
            auto newChilds = std::vector<LogicalOperator>{};
            /// Keep watermark assigner in same layout as join. Currently not working with random otherwise
            for (auto& child : logicalOperator.getChildren())
            {
                if (child.getName() == "EventTimeWatermarkAssigner")
                {
                    auto watermarkTraitSet = child.getTraitSet();
                    tryInsert(watermarkTraitSet, traitSet.tryGet<MemoryLayoutTypeTrait>().value());
                    newChilds.emplace_back(child.withTraitSet(watermarkTraitSet));
                }
                else
                {
                    newChilds.emplace_back(child);
                }
            }
            childs = newChilds;
        }
    }
    auto children = childs | std::views::transform([this, &conf](const LogicalOperator& child) { return apply(child, conf); })
        | std::ranges::to<std::vector>();

    /// insert memory swaps where needed (not for all row)
    if (conf.rowLayoutRatio.getValue() < 1.0)
    {
        if (logicalOperator.getName() != "Source")
        {
            auto newChilds = std::vector<LogicalOperator>{};
            for (const auto& child : children)
            {
                auto childLayout = child.getTraitSet().get<MemoryLayoutTypeTrait>().memoryLayout;
                auto operatorLayout = traitSet.get<MemoryLayoutTypeTrait>().memoryLayout;
                if (childLayout != operatorLayout)
                {
                    if (child.getName() == "Source")
                    { /// insert swap using rawScan
                        auto memorySwap = MemorySwapLogicalOperator(
                            child.getOutputSchema(),
                            MemoryLayoutType::ROW_LAYOUT,
                            operatorLayout,
                            child.getAs<SourceDescriptorLogicalOperator>()->getSourceDescriptor().getParserConfig());
                        newChilds.emplace_back(memorySwap.withChildren({child}).withTraitSet(child.getTraitSet()));
                    }
                    else
                    {
                        auto memorySwap = MemorySwapLogicalOperator(child.getOutputSchema(), childLayout, operatorLayout);
                        newChilds.emplace_back(memorySwap.withChildren({child}).withTraitSet(child.getTraitSet()));
                    }
                }
                else
                {
                    newChilds.emplace_back(child);
                }
            }
            return logicalOperator.withChildren(newChilds).withTraitSet(traitSet);
        }
    }

    return logicalOperator.withChildren(children).withTraitSet(traitSet);
}
}

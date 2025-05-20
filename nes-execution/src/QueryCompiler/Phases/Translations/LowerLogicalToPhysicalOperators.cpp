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
#include <memory>
#include <ranges>
#include <utility>
#include <vector>

#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/LogicalSelectionOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Phases/Translations/LowerLogicalToPhysicalOperators.hpp>
#include <QueryCompiler/Phases/Translations/PhysicalOperatorProvider.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::QueryCompilation
{

std::shared_ptr<LowerLogicalToPhysicalOperators>
LowerLogicalToPhysicalOperators::LowerLogicalToPhysicalOperators::create(const std::shared_ptr<PhysicalOperatorProvider>& provider)
{
    return std::make_shared<LowerLogicalToPhysicalOperators>(provider);
}

LowerLogicalToPhysicalOperators::LowerLogicalToPhysicalOperators(std::shared_ptr<PhysicalOperatorProvider> provider)
    : provider(std::move(provider))
{
}

std::shared_ptr<DecomposedQueryPlan> LowerLogicalToPhysicalOperators::apply(std::shared_ptr<DecomposedQueryPlan> decomposedQueryPlan) const
{
    auto isAlreadyLowered = [](const auto& node)
    {
        return not(
            Util::instanceOf<PhysicalOperators::PhysicalOperator>(node) or Util::instanceOf<SourceDescriptorLogicalOperator>(node)
            or Util::instanceOf<SinkLogicalOperator>(node));
    };
    const std::vector<std::shared_ptr<Node>> nodes = PlanIterator(*decomposedQueryPlan).snapshot();
    for (const auto& node : nodes | std::views::filter(isAlreadyLowered))
    {
        if (Util::instanceOf<LogicalSelectionOperator>(node))
        {
          auto filterOperator=  Util::as<LogicalSelectionOperator>(node);
          auto fieldNames = filterOperator->getFieldNamesUsedByFilterPredicate();
          auto schema = Schema::create(filterOperator->getOutputSchema()->getLayoutType());
          /// add only fields used by filter predicate to schema
          for (const auto& fieldName : fieldNames)
          {
            const auto field = filterOperator->getOutputSchema()->getFieldByName(fieldName);
            if (field.has_value())
                schema->addField(field.value());
          }
          filterOperator->setOutputSchema(schema);
          /// update sink to receive altered fields
          auto sinkSchema = schema->copy();
          sinkSchema->setLayoutType(Schema::MemoryLayoutType::ROW_LAYOUT);
            for (const auto& parent : decomposedQueryPlan->getRootOperators())
            {
                if (Util::instanceOf<SinkLogicalOperator>(parent))
                {
                    auto sink = Util::as<SinkLogicalOperator>(parent);
                    sink->sinkDescriptor->schema = sinkSchema;
                    sink->setInputSchema(sinkSchema);
                    sink->setOutputSchema(sinkSchema);
                    //decomposedQueryPlan->replaceRootOperator(parent, sink);
                }
            }
        }
        provider->lower(*decomposedQueryPlan, NES::Util::as<LogicalOperator>(node));
    }
    return decomposedQueryPlan;
}

}

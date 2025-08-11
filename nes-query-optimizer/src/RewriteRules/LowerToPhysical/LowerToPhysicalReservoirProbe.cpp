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

#include <RewriteRules/LowerToPhysical/LowerToPhysicalReservoirProbe.hpp>

#include <memory>
#include <Aggregation/Function/Synopsis/Sample/SampleProbePhysicalOperator.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/Aggregations/Synopsis/Sample/ReservoirProbeLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>

namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalReservoirProbe::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGet<ReservoirProbeLogicalOperator>(), "Expected a ReservoirProbeLogicalOperator");
    auto reservoirProbe = logicalOperator.get<ReservoirProbeLogicalOperator>();
    auto schema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT};
    schema.addField("stream$id", DataType::Type::UINT64);
    schema.addField("stream$value", DataType::Type::UINT64);
    schema.addField("stream$timestamp", DataType::Type::UINT64);
    auto asField = reservoirProbe.asField.getFieldName();
    /// TODO get these from the logical operator!
    const auto windowMetaData = WindowMetaData{"stream$start", "stream$end"};
    auto physicalOperator = SampleProbePhysicalOperator(schema, asField, windowMetaData);
    auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        logicalOperator.getInputSchemas()[0],
        logicalOperator.getOutputSchema(),
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leaves(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leafs = {leaves}};
}

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterReservoirProbeRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalReservoirProbe>(argument.conf);
}
}

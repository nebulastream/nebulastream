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
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <RewriteRules/LowerToPhysical/LowerToPhysicalSource.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include <SourcePhysicalOperator.hpp>

namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalSource::apply(LogicalOperator logicalOperator, QueryId)
{
    PRECONDITION(logicalOperator.tryGet<SourceDescriptorLogicalOperator>(), "Expected a SourceDescriptorLogicalOperator");
    const auto source = logicalOperator.get<SourceDescriptorLogicalOperator>();

    const auto outputOriginIds = source.getOutputOriginIds();
    PRECONDITION(
        outputOriginIds.size() == 1,
        "SourceDescriptorLogicalOperator should have exactly one origin id, but has {}",
        outputOriginIds.size());
    auto physicalOperator = SourcePhysicalOperator(source.getSourceDescriptor(), outputOriginIds[0]);

    const auto inputSchemas = logicalOperator.getInputSchemas();
    PRECONDITION(
        inputSchemas.size() == 1, "SourceDescriptorLogicalOperator should have exactly one schema, but has {}", inputSchemas.size());
    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator, inputSchemas[0], logicalOperator.getOutputSchema(), PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
    return {.root = wrapper, .leafs = {}};
}

RewriteRuleRegistryReturnType RewriteRuleGeneratedRegistrar::RegisterSourceRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalSource>(argument.conf);
}
}

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
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <RewriteRules/LowerToPhysical/LowerToPhysicalSource.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include <SourcePhysicalOperator.hpp>
#include <ErrorHandling.hpp>
#include <Operators/LogicalOperator.hpp>

namespace NES::Optimizer
{

RewriteRuleResultSubgraph LowerToPhysicalSource::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGet<SourceDescriptorLogicalOperator>(), "Expected a SourceDescriptorLogicalOperator");
    auto source = logicalOperator.get<SourceDescriptorLogicalOperator>();

    auto outputOriginIds = source.getOutputOriginIds();
    PRECONDITION(outputOriginIds.size() == 1, "SourceDescriptorLogicalOperator should have exactly one origin id, but has {}", outputOriginIds.size());
    auto physicalOperator = SourcePhysicalOperator(source.getSourceDescriptor(), outputOriginIds[0]);

    auto inputSchemas = logicalOperator.getInputSchemas();
    PRECONDITION(inputSchemas.size() == 1, "SourceDescriptorLogicalOperator should have exactly one schema, but has {}", inputSchemas.size());
    auto wrapper = std::make_shared<PhysicalOperatorWrapper>(physicalOperator, inputSchemas[0], logicalOperator.getOutputSchema());
    return {wrapper, {}};
}

RewriteRuleRegistryReturnType RewriteRuleGeneratedRegistrar::RegisterSourceRewriteRule(RewriteRuleRegistryArguments argument)
{
    return std::make_unique<LowerToPhysicalSource>(argument.conf);
}
}

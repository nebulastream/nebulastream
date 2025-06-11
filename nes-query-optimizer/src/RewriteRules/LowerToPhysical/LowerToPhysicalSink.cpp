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

#include <RewriteRules/LowerToPhysical/LowerToPhysicalSink.hpp>

#include <memory>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include <SinkPhysicalOperator.hpp>

namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalSink::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGet<SinkLogicalOperator>(), "Expected a SinkLogicalOperator");
    auto sink = logicalOperator.get<SinkLogicalOperator>();
    PRECONDITION(sink.sinkDescriptor.has_value(), "Expected SinkLogicalOperator to have sink descriptor");
    auto physicalOperator = SinkPhysicalOperator(sink.sinkDescriptor.value());
    auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator, sink.getInputSchemas()[0], sink.getOutputSchema(), PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
    ;

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leafes(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leafs = {leafes}};
}

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterSinkRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalSink>(argument.conf);
}
}

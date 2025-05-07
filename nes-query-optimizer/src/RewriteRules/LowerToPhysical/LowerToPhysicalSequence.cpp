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

#include <RewriteRules/LowerToPhysical/LowerToPhysicalSequence.hpp>

#include <memory>
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SequenceLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include <SequenceOperatorHandler.hpp>
#include <SequencePhysicalOperator.hpp>
#include <SinkPhysicalOperator.hpp>

namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalSequence::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGet<SequenceLogicalOperator>(), "Expected a SequenceLogicalOperator");
    auto sink = logicalOperator.get<SequenceLogicalOperator>();

    auto schema = logicalOperator.getInputSchemas().at(0);

    auto layout = std::make_shared<Memory::MemoryLayouts::RowLayout>(conf.operatorBufferSize.getValue(), schema);
    const auto memoryProvider = std::make_shared<Interface::MemoryProvider::RowTupleBufferMemoryProvider>(layout);

    auto operatorHandlerId = getNextOperatorHandlerId();
    auto handler = std::make_shared<Runtime::Execution::Operators::SequenceOperatorHandler>();
    auto physicalOperator = Runtime::Execution::Operators::SequencePhysicalOperator(
        operatorHandlerId, ScanPhysicalOperator(memoryProvider, schema.getFieldNames()));

    auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        sink.getInputSchemas()[0],
        sink.getOutputSchema(),
        operatorHandlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::SCAN);

    return {.root = wrapper, .leafs = {wrapper}};
}

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterSequenceRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalSequence>(argument.conf);
}
}

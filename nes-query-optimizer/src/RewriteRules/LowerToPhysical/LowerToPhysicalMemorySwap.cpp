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

#include <RewriteRules/LowerToPhysical/LowerToPhysicalMemorySwap.hpp>

#include <memory>
#include <Functions/FunctionProvider.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/MemorySwapLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <EmitOperatorHandler.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterTupleBufferRefProvider.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include <ScanPhysicalOperator.hpp>

namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalMemorySwap::apply(LogicalOperator logicalOperator)
{
    auto logicalMemorySwap = logicalOperator.getAs<MemorySwapLogicalOperator>();
    auto inputLayout = logicalMemorySwap->getInputMemoryLayoutType();
    auto outputLayout = logicalMemorySwap->getOutputMemoryLayoutType();
    auto schema = logicalOperator.getInputSchemas()[0]; /// assuming in and putput schema is the same
    auto bufferSize = conf.operatorBufferSize.getValue();


    auto incomingProvider = LowerSchemaProvider::lowerSchema(bufferSize, schema, inputLayout);

    auto targetProvider = LowerSchemaProvider::lowerSchema(bufferSize, schema, outputLayout);

    auto scan = ScanPhysicalOperator(incomingProvider, schema.getFieldNames());
    auto scanWrapper = std::make_shared<PhysicalOperatorWrapper>(
        scan, schema, schema, inputLayout, inputLayout, PhysicalOperatorWrapper::PipelineLocation::SCAN);


    if (!logicalMemorySwap->getInputFormatterConfig().parserType.empty()
        && toUpperCase(logicalMemorySwap->getInputFormatterConfig().parserType) != "NATIVE")
    {
        auto scanFirst = ScanPhysicalOperator(
            provideInputFormatterTupleBufferRef(logicalMemorySwap->getInputFormatterConfig(), incomingProvider), schema.getFieldNames());
        scanWrapper = std::make_shared<PhysicalOperatorWrapper>(
            scanFirst, schema, schema, inputLayout, inputLayout, PhysicalOperatorWrapper::PipelineLocation::SCAN);
    }
    auto handlerId = getNextOperatorHandlerId();
    auto emit = EmitPhysicalOperator(handlerId, targetProvider);
    auto emitWrapper = std::make_shared<PhysicalOperatorWrapper>(
        emit,
        schema,
        schema,
        outputLayout,
        outputLayout,
        handlerId,
        std::make_shared<EmitOperatorHandler>(),
        PhysicalOperatorWrapper::PipelineLocation::EMIT);

    emitWrapper->addChild(scanWrapper);

    std::vector leafes(logicalOperator.getChildren().size(), scanWrapper);

    return {.root = emitWrapper, .leafs = {leafes}};
};

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterMemorySwapRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalMemorySwap>(argument.conf);
}
}

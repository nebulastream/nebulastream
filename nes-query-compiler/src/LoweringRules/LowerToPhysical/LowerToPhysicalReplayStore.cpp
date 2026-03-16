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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalReplayStore.hpp>

#include <memory>
#include <sstream>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ReplayStoreLogicalOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <ReplayStoreOperatorHandler.hpp>
#include <ReplayStorePhysicalOperator.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalReplayStore::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<ReplayStoreLogicalOperator>(), "Expected a ReplayStoreLogicalOperator");
    auto store = logicalOperator.getAs<ReplayStoreLogicalOperator>();

    auto cfgCopy = DescriptorConfig::Config(store->getConfig());
    Descriptor logicalCfg(std::move(cfgCopy));
    const auto filePath = logicalCfg.getFromConfig(StoreLogicalOperator::ConfigParameters::FILE_PATH);

    std::stringstream schemaStream;
    schemaStream << logicalOperator.getOutputSchema();

    const ReplayStoreOperatorHandler::Config handlerCfg{
        .filePath = filePath,
        .schemaText = schemaStream.str(),
    };

    auto handlerId = getNextOperatorHandlerId();
    auto handler = std::make_shared<ReplayStoreOperatorHandler>(handlerCfg);

    const auto inputSchema = logicalOperator.getInputSchemas()[0];
    const auto outputSchema = logicalOperator.getOutputSchema();
    auto physicalOperator = ReplayStorePhysicalOperator(handlerId, inputSchema);
    const auto memoryLayoutTypeTrait = logicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;
    auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        inputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    const std::vector leafs{wrapper};
    return {.root = wrapper, .leafs = leafs};
}

std::unique_ptr<AbstractLoweringRule>
LoweringRuleGeneratedRegistrar::RegisterReplayStoreLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalReplayStore>(argument.conf);
}

}

#include <string>
#include <vector>
#include <Utils.hpp>

#include <DataTypes/Schema.hpp>
#include <Functions/ConstantValuePhysicalFunction.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/BooleanFunctions/AndPhysicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsPhysicalFunction.hpp>
#include <Functions/BooleanFunctions/OrPhysicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterEqualsPhysicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterPhysicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessEqualsPhysicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessPhysicalFunction.hpp>
#include <Nautilus/Interface/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Util/Common.hpp>

#include <EmitOperatorHandler.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ScanPhysicalOperator.hpp>
#include <SelectionPhysicalOperator.hpp>
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
namespace NES
{
    std::pair<std::shared_ptr<PhysicalOperatorWrapper>, std::shared_ptr<PhysicalOperatorWrapper>> addScanAndEmitAroundOperator(const std::shared_ptr<PhysicalOperatorWrapper>& oldOperatorWrapper, MemoryLayoutTypeTrait memoryLayoutTrait, QueryExecutionConfiguration conf, Schema sourceSchema)
    {
        auto res= addSwapBeforeOperator(oldOperatorWrapper, memoryLayoutTrait, conf, sourceSchema);

        auto operatorWrapper= res.first;
        auto swapScanWrapper= res.second;

        auto inputSchema = operatorWrapper->getInputSchema().value();
        auto outputSchema = operatorWrapper->getOutputSchema().value();

        auto provider = Interface::MemoryProvider::TupleBufferMemoryProvider::create(conf.operatorBufferSize, inputSchema);

        auto scan = ScanPhysicalOperator(provider, outputSchema.getFieldNames());
        auto handlerId = getNextOperatorHandlerId();
        auto emit = EmitPhysicalOperator(handlerId, provider);
        auto scanWrapper = std::make_shared<PhysicalOperatorWrapper>(
            scan, inputSchema, outputSchema, PhysicalOperatorWrapper::PipelineLocation::SCAN);

        auto emitWrapper = std::make_shared<PhysicalOperatorWrapper>(
            emit, outputSchema, outputSchema, handlerId, std::make_shared<EmitOperatorHandler>(),
            PhysicalOperatorWrapper::PipelineLocation::EMIT);

        auto children = operatorWrapper->getChildren();
        //filter -> emit -> scan -> else


        scanWrapper->addChild(children[0]);
        operatorWrapper->setChildren({scanWrapper});
        emitWrapper->addChild(operatorWrapper);
        // emit -> filter -> scan -> emit -> scan
        return {emitWrapper, swapScanWrapper};
    }

    std::pair<std::shared_ptr<PhysicalOperatorWrapper>, std::shared_ptr<PhysicalOperatorWrapper>> addSwapBeforeOperator(const std::shared_ptr<PhysicalOperatorWrapper>& operatorWrapper, MemoryLayoutTypeTrait memoryLayoutTrait, QueryExecutionConfiguration conf)
    {
        auto inputSchema = operatorWrapper->getInputSchema().value().withMemoryLayoutType(memoryLayoutTrait.incomingLayoutType);

        return addSwapBeforeOperator(operatorWrapper, memoryLayoutTrait, conf, inputSchema);
    }


    std::pair<std::shared_ptr<PhysicalOperatorWrapper>, std::shared_ptr<PhysicalOperatorWrapper>> addSwapBeforeOperator(const std::shared_ptr<PhysicalOperatorWrapper>& operatorWrapper, MemoryLayoutTypeTrait memoryLayoutTrait, QueryExecutionConfiguration conf, Schema inputSchema)
    {
        inputSchema = inputSchema.withMemoryLayoutType(memoryLayoutTrait.incomingLayoutType);
        auto targetSchema = operatorWrapper->getInputSchema().value().withMemoryLayoutType(memoryLayoutTrait.targetLayoutType);

        auto incomingProvider = Interface::MemoryProvider::TupleBufferMemoryProvider::create(conf.operatorBufferSize, inputSchema); //all fields
        auto targetProvider = Interface::MemoryProvider::TupleBufferMemoryProvider::create(conf.operatorBufferSize, targetSchema); //sink fields

        auto scan = ScanPhysicalOperator(incomingProvider, targetSchema.getFieldNames()); // all fields, but only sink fields projections
        auto handlerId = getNextOperatorHandlerId();
        auto emit = EmitPhysicalOperator(handlerId, targetProvider);
        auto scanWrapper = std::make_shared<PhysicalOperatorWrapper>(
            scan, inputSchema, inputSchema, PhysicalOperatorWrapper::PipelineLocation::SCAN);//TODO: outputSchema to targetSchema? (verm. egal)

        auto emitWrapper = std::make_shared<PhysicalOperatorWrapper>(
            emit, targetSchema, targetSchema, handlerId, std::make_shared<EmitOperatorHandler>(),
            PhysicalOperatorWrapper::PipelineLocation::EMIT);

        operatorWrapper->addChild(emitWrapper);
        emitWrapper->addChild(scanWrapper);

        return {operatorWrapper, scanWrapper};
    }

    MemoryLayoutTypeTrait getMemoryLayoutTypeTrait(const LogicalOperator& logicalOperator)
    {
        auto traitSet = logicalOperator.getTraitSet();

        const auto memoryLayoutIter
            = std::ranges::find_if(traitSet, [](const Trait& trait) { return trait.tryGet<MemoryLayoutTypeTrait>().has_value(); });
        PRECONDITION(memoryLayoutIter != traitSet.end(), "operator must have a memory layout type trait");
        auto memoryLayoutTrait = memoryLayoutIter->get<MemoryLayoutTypeTrait>();
        return memoryLayoutTrait;
    }

    std::vector<std::string> getAccessedFieldNames(PhysicalFunction func)
    {
        if (func.tryGet<FieldAccessPhysicalFunction>())
        {
            auto fieldAccessFunc = func.get<FieldAccessPhysicalFunction>();
            return {fieldAccessFunc.getField()};
        }

        auto processChildren = [](auto&& specialized) {
            auto [leftFunc, rightFunc] = specialized.getChildFunctions();
            auto leftFields = getAccessedFieldNames(leftFunc);
            auto rightFields = getAccessedFieldNames(rightFunc);
            leftFields.insert(leftFields.end(), rightFields.begin(), rightFields.end());
            return leftFields;
        };

        if (auto andFunc = func.tryGet<AndPhysicalFunction>())
        {
            return processChildren(*andFunc);
        }
        if (auto orFunc = func.tryGet<OrPhysicalFunction>())
        {
            return processChildren(*orFunc);
        }
        if (auto greaterFunc = func.tryGet<GreaterPhysicalFunction>())
        {
            return processChildren(*greaterFunc);
        }
        if (auto greaterEqualFunc = func.tryGet<GreaterEqualsPhysicalFunction>())
        {
            return processChildren(*greaterEqualFunc);
        }
        if (auto lessFunc = func.tryGet<LessPhysicalFunction>())
        {
            return processChildren(*lessFunc);
        }
        if (auto lessEqualFunc = func.tryGet<LessEqualsPhysicalFunction>())
        {
            return processChildren(*lessEqualFunc);
        }
        if (auto equalFunc = func.tryGet<EqualsPhysicalFunction>())
        {
            return processChildren(*equalFunc);
        }
        return {};
    }

    std::vector<Record::RecordFieldIdentifier> getVectorDifference(
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const std::vector<Record::RecordFieldIdentifier>& fieldNames)
    {
        std::vector<Record::RecordFieldIdentifier> difference;
        for (const auto& proj : projections)
        {
            if (std::find(fieldNames.begin(), fieldNames.end(), proj) == fieldNames.end())
            {
                difference.push_back(proj);
            }
        }
        return difference;
    }
}

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

#include <cstddef>
#include <memory>
#include <numeric>
#include <utility>
#include <Configurations/Worker/QueryOptimizerConfiguration.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <RewriteRules/LowerToPhysical/LowerToPhysicalWindowedAggregation.hpp>
#include <Streaming/Aggregation/AggregationBuildPhysicalOperator.hpp>
#include <Streaming/Aggregation/AggregationProbePhysicalOperator.hpp>
#include <Streaming/Aggregation/Function/AvgAggregationFunction.hpp>
#include <Streaming/Aggregation/Function/CountAggregationFunction.hpp>
#include <Streaming/Aggregation/Function/MaxAggregationFunction.hpp>
#include <Streaming/Aggregation/Function/MedianAggregationFunction.hpp>
#include <Streaming/Aggregation/Function/MinAggregationFunction.hpp>
#include <Streaming/Aggregation/Function/SumAggregationFunction.hpp>
#include <Streaming/Aggregation/WindowAggregation.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <RewriteRuleRegistry.hpp>
#include <magic_enum/magic_enum.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Watermark/TimeFunction.hpp>

namespace NES::Optimizer
{

std::pair<std::vector<Nautilus::Record::RecordFieldIdentifier>, std::vector<Nautilus::Record::RecordFieldIdentifier>>
getKeyAndValueFields(const WindowedAggregationLogicalOperator& logicalOperator)
{
    std::vector<Nautilus::Record::RecordFieldIdentifier> fieldKeyNames;
    std::vector<Nautilus::Record::RecordFieldIdentifier> fieldValueNames;

    /// Getting the key and value field names
    for (const auto& nodeAccess : logicalOperator.getKeys())
    {
        fieldKeyNames.emplace_back(nodeAccess.getFieldName());
    }
    for (const auto& descriptor : logicalOperator.getWindowAggregation())
    {
        const auto aggregationResultFieldIdentifier = descriptor->on().getFieldName();
        fieldValueNames.emplace_back(aggregationResultFieldIdentifier);
    }
    return {fieldKeyNames, fieldValueNames};
}

std::shared_ptr<TimeFunction> getTimeFunction(const WindowedAggregationLogicalOperator& logicalOperator)
{
    const auto timeWindow = dynamic_cast<Windowing::TimeBasedWindowType*>(&logicalOperator.getWindowType());
    if (not timeWindow)
    {
        throw UnknownWindowType("Window type is not a time based window type");
    }

    switch (timeWindow->getTimeCharacteristic().getType())
    {
        case Windowing::TimeCharacteristic::Type::IngestionTime: {
            if (timeWindow->getTimeCharacteristic().getField().getName() == Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME)
            {
                return std::make_shared<IngestionTimeFunction>();
            }
            throw UnknownWindowType(
                "The ingestion time field of a window must be: {}", Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME);
        }
        case Windowing::TimeCharacteristic::Type::EventTime: {
            /// For event time fields, we look up the reference field name and create an expression to read the field.
            auto timeCharacteristicField = timeWindow->getTimeCharacteristic().getField().getName();
            auto timeStampField = Functions::FieldAccessPhysicalFunction(timeCharacteristicField);
            return std::make_shared<EventTimeFunction>(timeStampField, timeWindow->getTimeCharacteristic().getTimeUnit());
        }
        default: {
            throw UnknownWindowType("Unknown window type: {}", magic_enum::enum_name(timeWindow->getTimeCharacteristic().getType()));
        }
    }
}

std::vector<std::shared_ptr<AggregationFunction>>
getAggregationFunctions(const WindowedAggregationLogicalOperator& logicalOperator, const NES::Configurations::QueryOptimizerConfiguration& config)
{
    std::vector<std::shared_ptr<AggregationFunction>> aggregationFunctions;
    const auto& aggregationDescriptors = logicalOperator.getWindowAggregation();
    for (const auto& descriptor : aggregationDescriptors)
    {
        const DefaultPhysicalTypeFactory physicalTypeFactory;

        auto physicalInputType = physicalTypeFactory.getPhysicalType(descriptor->getInputStamp());
        auto physicalFinalType = physicalTypeFactory.getPhysicalType(descriptor->getFinalAggregateStamp());

        auto aggregationInputExpression = QueryCompilation::FunctionProvider::lowerFunction(descriptor->on());
        const auto aggregationResultFieldIdentifier = descriptor->as().getFieldName();
        switch (descriptor->getType())
        {
            case WindowAggregationLogicalFunction::Type::Avg: {
                /// We assume that the count is a u64
                auto countType = physicalTypeFactory.getPhysicalType(*DataTypeProvider::provideDataType(LogicalType::UINT64));
                aggregationFunctions.emplace_back(
                    std::make_shared<AvgAggregationFunction>(
                        std::move(physicalInputType),
                        std::move(physicalFinalType),
                        std::move(aggregationInputExpression),
                        aggregationResultFieldIdentifier,
                        std::move(countType)));
                break;
            }
            case WindowAggregationLogicalFunction::Type::Sum: {
                aggregationFunctions.emplace_back(
                    std::make_shared<SumAggregationFunction>(
                        std::move(physicalInputType),
                        std::move(physicalFinalType),
                        std::move(aggregationInputExpression),
                        aggregationResultFieldIdentifier));
                break;
            }
            case WindowAggregationLogicalFunction::Type::Count: {
                /// We assume that a count is a u64
                auto countType = physicalTypeFactory.getPhysicalType(*DataTypeProvider::provideDataType(LogicalType::UINT64));
                aggregationFunctions.emplace_back(
                    std::make_shared<CountAggregationFunction>(
                        std::move(countType),
                        std::move(physicalFinalType),
                        std::move(aggregationInputExpression),
                        aggregationResultFieldIdentifier));
                break;
            }
            case WindowAggregationLogicalFunction::Type::Max: {
                aggregationFunctions.emplace_back(
                    std::make_shared<MaxAggregationFunction>(
                        std::move(physicalInputType),
                        std::move(physicalFinalType),
                        std::move(aggregationInputExpression),
                        aggregationResultFieldIdentifier));
                break;
            }
            case WindowAggregationLogicalFunction::Type::Min: {
                aggregationFunctions.emplace_back(
                    std::make_shared<MinAggregationFunction>(
                        std::move(physicalInputType),
                        std::move(physicalFinalType),
                        std::move(aggregationInputExpression),
                        aggregationResultFieldIdentifier));
                break;
            }
            case WindowAggregationLogicalFunction::Type::Median: {
                auto layout = std::make_shared<Memory::MemoryLayouts::ColumnLayout>(
                    logicalOperator.getInputSchemas()[0], config.pageSize.getValue());
                auto memoryProvider = std::make_unique<ColumnTupleBufferMemoryProvider>(std::move(layout));
                aggregationFunctions.emplace_back(
                    std::make_shared<MedianAggregationFunction>(
                        std::move(physicalInputType),
                        std::move(physicalFinalType),
                        std::move(aggregationInputExpression),
                        aggregationResultFieldIdentifier,
                        std::move(memoryProvider)));
                break;
            }
            default: {
                throw NotImplemented();
                break;
            }
        }
    }
    return aggregationFunctions;
}

RewriteRuleResult LowerToPhysicalWindowedAggregation::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGet<WindowedAggregationLogicalOperator>(), "Expected a WindowedAggregationLogicalOperator");
    const size_t operatorHandlerIndex = 0; /// TODO this should be a singleton as for the queryid?

    auto aggregation = logicalOperator.get<WindowedAggregationLogicalOperator>();

    const auto inSchema = logicalOperator.getInputSchemas()[0];
    const auto outSchema = logicalOperator.getOutputSchema();

    auto& aggregationFunctions = aggregation.getWindowAggregation();
    auto timeFunction = getTimeFunction(aggregation);

    auto aggregationFunctions_ = getAggregationFunctions(aggregation, conf);

    const auto valueSize = std::accumulate(
        aggregationFunctions_.begin(),
        aggregationFunctions_.end(),
        0,
        [](const auto& sum, const auto& function) { return sum + function->getSizeOfStateInBytes(); });

    std::vector<Functions::PhysicalFunction> keyFunctions;
    uint64_t keySize = 0;
    auto keyFunctionLogical = aggregation.getKeys();
    for (auto& nodeFunctionKey : keyFunctionLogical)
    {
        const DefaultPhysicalTypeFactory typeFactory;
        const auto& loweredFunctionType = nodeFunctionKey.getStamp();
        keyFunctions.emplace_back(QueryCompilation::FunctionProvider::lowerFunction(nodeFunctionKey));
        keySize += typeFactory.getPhysicalType(loweredFunctionType)->size();
    }
    const auto entrySize = sizeof(Nautilus::Interface::ChainedHashMapEntry) + keySize + valueSize;
    const auto entriesPerPage = conf.pageSize.getValue() / entrySize;

    const auto& [fieldKeyNames, fieldValueNames] = getKeyAndValueFields(aggregation);
    const auto& [fieldKeys, fieldValues] = ChainedEntryMemoryProvider::createFieldOffsets(inSchema, fieldKeyNames, fieldValueNames);

    auto windowAggregationPhysicalOperator = std::make_shared<WindowAggregation>(
        aggregationFunctions_,
        std::make_shared<Nautilus::Interface::MurMur3HashFunction>(),
        fieldKeys,
        fieldValues,
        entriesPerPage,
        entrySize);

    auto build = AggregationBuildPhysicalOperator(
        operatorHandlerIndex, timeFunction, keyFunctions,
        windowAggregationPhysicalOperator);

    auto probe = AggregationProbePhysicalOperator(windowAggregationPhysicalOperator, operatorHandlerIndex, aggregation.getWindowStartFieldName(), aggregation.getWindowEndFieldName());

    auto buildPhysicalOp = std::make_shared<PhysicalOperatorWrapper>(build, aggregation.getInputSchemas()[0], aggregation.getOutputSchema());
    auto probePhysicalOp = std::make_shared<PhysicalOperatorWrapper>(probe, aggregation.getInputSchemas()[0], aggregation.getOutputSchema());
    buildPhysicalOp->children.push_back(probePhysicalOp);

    return {buildPhysicalOp, {probePhysicalOp}};
}

std::unique_ptr<Optimizer::AbstractRewriteRule> Optimizer::RewriteRuleGeneratedRegistrar::RegisterWindowedAggregationRewriteRule(RewriteRuleRegistryArguments argument)
{
    return std::make_unique<NES::Optimizer::LowerToPhysicalWindowedAggregation>(argument.conf);
}
}

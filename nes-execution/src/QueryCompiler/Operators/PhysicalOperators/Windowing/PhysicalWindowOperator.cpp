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
#include <ostream>
#include <utility>
#include <vector>
#include <API/Schema.hpp>
#include <Execution/Functions/ExecutableFunctionReadField.hpp>
#include <Execution/Operators/Streaming/Aggregation/Function/AggregationFunction.hpp>
#include <Execution/Operators/Streaming/Aggregation/Function/AvgAggregationFunction.hpp>
#include <Execution/Operators/Streaming/Aggregation/Function/CountAggregationFunction.hpp>
#include <Execution/Operators/Streaming/Aggregation/Function/MaxAggregationFunction.hpp>
#include <Execution/Operators/Streaming/Aggregation/Function/MedianAggregationFunction.hpp>
#include <Execution/Operators/Streaming/Aggregation/Function/MergeAggregationFunction.hpp>
#include <Execution/Operators/Streaming/Aggregation/Function/MinAggregationFunction.hpp>
#include <Execution/Operators/Streaming/Aggregation/Function/SumAggregationFunction.hpp>
#include <Execution/Operators/Streaming/WindowBasedOperatorHandler.hpp>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <MemoryLayout/ColumnLayout.hpp>
#include <Nautilus/Interface/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <QueryCompiler/Configurations/QueryCompilerConfiguration.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>
#include <QueryCompiler/Phases/Translations/FunctionProvider.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Util/Common.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalWindowOperator::PhysicalWindowOperator(
    const OperatorId id,
    std::shared_ptr<Schema> inputSchema,
    std::shared_ptr<Schema> outputSchema,
    std::shared_ptr<Windowing::LogicalWindowDescriptor> windowDefinition,
    std::shared_ptr<Runtime::Execution::Operators::WindowBasedOperatorHandler> windowHandler)
    : Operator(id)
    , PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema))
    , windowDefinition(std::move(windowDefinition))
    , windowHandler(std::move(windowHandler))
{
}

const std::shared_ptr<Windowing::LogicalWindowDescriptor>& PhysicalWindowOperator::getWindowDefinition() const
{
    return windowDefinition;
};

std::unique_ptr<Runtime::Execution::Operators::TimeFunction> PhysicalWindowOperator::getTimeFunction() const
{
    const auto timeWindow = NES::Util::as_if<Windowing::TimeBasedWindowType>(windowDefinition->getWindowType());
    if (not timeWindow)
    {
        throw UnknownWindowType("Window type is not a time based window type");
    }

    switch (timeWindow->getTimeCharacteristic()->getType())
    {
        case Windowing::TimeCharacteristic::Type::IngestionTime: {
            if (timeWindow->getTimeCharacteristic()->field->getName() == Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME)
            {
                return std::make_unique<Runtime::Execution::Operators::IngestionTimeFunction>();
            }
            throw UnknownWindowType(
                "The ingestion time field of a window must be: {}", Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME);
        }
        case Windowing::TimeCharacteristic::Type::EventTime: {
            /// For event time fields, we look up the reference field name and create an expression to read the field.
            auto timeCharacteristicField = timeWindow->getTimeCharacteristic()->field->getName();
            auto timeStampField = std::make_unique<Runtime::Execution::Functions::ExecutableFunctionReadField>(timeCharacteristicField);
            return std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(
                std::move(timeStampField), timeWindow->getTimeCharacteristic()->getTimeUnit());
        }
        default: {
            throw UnknownWindowType("Unknown window type: {}", magic_enum::enum_name(timeWindow->getTimeCharacteristic()->getType()));
        }
    }
}

std::pair<std::vector<Nautilus::Record::RecordFieldIdentifier>, std::vector<Nautilus::Record::RecordFieldIdentifier>>
PhysicalWindowOperator::getKeyAndValueFields() const
{
    const auto aggregationDescriptors = getWindowDefinition()->getWindowAggregation();
    std::vector<Nautilus::Record::RecordFieldIdentifier> fieldKeyNames;
    std::vector<Nautilus::Record::RecordFieldIdentifier> fieldValueNames;

    /// Getting the key and value field names
    for (const auto& nodeAccess : getWindowDefinition()->getKeys())
    {
        fieldKeyNames.emplace_back(nodeAccess->getFieldName());
    }
    for (const auto& descriptor : aggregationDescriptors)
    {
        if (const auto fieldAccessExpression = NES::Util::as_if<NodeFunctionFieldAccess>(descriptor->on()))
        {
            const auto aggregationResultFieldIdentifier = fieldAccessExpression->getFieldName();
            fieldValueNames.emplace_back(aggregationResultFieldIdentifier);
        }
        else
        {
            throw NotImplemented("Currently complex expression in as fields are not supported");
        }
    }

    return {fieldKeyNames, fieldValueNames};
}

std::vector<std::shared_ptr<Runtime::Execution::Aggregation::AggregationFunction>>
PhysicalWindowOperator::getAggregationFunctions(const Configurations::QueryCompilerConfiguration& options) const
{
    std::vector<std::shared_ptr<Runtime::Execution::Aggregation::AggregationFunction>> aggregationFunctions;
    const auto aggregationDescriptors = getWindowDefinition()->getWindowAggregation();
    for (const auto& descriptor : aggregationDescriptors)
    {
        const DefaultPhysicalTypeFactory physicalTypeFactory;

        auto physicalInputType = physicalTypeFactory.getPhysicalType(descriptor->getInputStamp());
        auto physicalFinalType = physicalTypeFactory.getPhysicalType(descriptor->getFinalAggregateStamp());

        auto aggregationInputExpression = FunctionProvider::lowerFunction(descriptor->on());
        if (const auto fieldAccessExpression = NES::Util::as_if<NodeFunctionFieldAccess>(descriptor->as()))
        {
            const auto aggregationResultFieldIdentifier = fieldAccessExpression->getFieldName();
            switch (descriptor->getType())
            {
                case Windowing::WindowAggregationDescriptor::Type::Avg: {
                    /// We assume that the count is a u64
                    const auto countType = physicalTypeFactory.getPhysicalType(DataTypeProvider::provideDataType(LogicalType::UINT64));
                    aggregationFunctions.emplace_back(std::make_shared<Runtime::Execution::Aggregation::AvgAggregationFunction>(
                        physicalInputType,
                        physicalFinalType,
                        std::move(aggregationInputExpression),
                        aggregationResultFieldIdentifier,
                        countType));
                    break;
                }
                case Windowing::WindowAggregationDescriptor::Type::Sum: {
                    aggregationFunctions.emplace_back(std::make_shared<Runtime::Execution::Aggregation::SumAggregationFunction>(
                        physicalInputType, physicalFinalType, std::move(aggregationInputExpression), aggregationResultFieldIdentifier));
                    break;
                }
                case Windowing::WindowAggregationDescriptor::Type::Count: {
                    /// We assume that a count is a u64
                    const auto countType = physicalTypeFactory.getPhysicalType(DataTypeProvider::provideDataType(LogicalType::UINT64));
                    aggregationFunctions.emplace_back(std::make_shared<Runtime::Execution::Aggregation::CountAggregationFunction>(
                        countType, physicalFinalType, std::move(aggregationInputExpression), aggregationResultFieldIdentifier));
                    break;
                }
                case Windowing::WindowAggregationDescriptor::Type::Max: {
                    aggregationFunctions.emplace_back(std::make_shared<Runtime::Execution::Aggregation::MaxAggregationFunction>(
                        physicalInputType, physicalFinalType, std::move(aggregationInputExpression), aggregationResultFieldIdentifier));
                    break;
                }
                case Windowing::WindowAggregationDescriptor::Type::Min: {
                    aggregationFunctions.emplace_back(std::make_shared<Runtime::Execution::Aggregation::MinAggregationFunction>(
                        physicalInputType, physicalFinalType, std::move(aggregationInputExpression), aggregationResultFieldIdentifier));
                    break;
                }
                case Windowing::WindowAggregationDescriptor::Type::Median: {
                    auto layout = std::make_shared<Memory::MemoryLayouts::ColumnLayout>(inputSchema, options.pageSize.getValue());
                    const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider
                        = std::make_shared<Nautilus::Interface::MemoryProvider::ColumnTupleBufferMemoryProvider>(layout);
                    aggregationFunctions.emplace_back(std::make_shared<Runtime::Execution::Aggregation::MedianAggregationFunction>(
                        physicalInputType,
                        physicalFinalType,
                        std::move(aggregationInputExpression),
                        aggregationResultFieldIdentifier,
                        memoryProvider));
                    break;
                }
                case Windowing::WindowAggregationDescriptor::Type::Merge: {
                    auto [keys, values] = getKeyAndValueFields();
                    auto memoryLayoutSchema = Schema::create();
                    for (auto existingField : *inputSchema)
                    {
                        if (std::ranges::find(keys, existingField->getName()) != keys.end()
                            || std::ranges::find(values, existingField->getName()) != values.end())
                        {
                            memoryLayoutSchema->addField(existingField->deepCopy());
                        }
                    }

                    auto layout = std::make_shared<Memory::MemoryLayouts::ColumnLayout>(memoryLayoutSchema, options.pageSize.getValue());
                    const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider
                        = std::make_shared<Nautilus::Interface::MemoryProvider::ColumnTupleBufferMemoryProvider>(layout);
                    aggregationFunctions.emplace_back(std::make_unique<Runtime::Execution::Aggregation::MergeAggregationFunction>(
                        physicalInputType,
                        physicalFinalType,
                        std::move(aggregationInputExpression),
                        aggregationResultFieldIdentifier,
                        memoryProvider));
                    break;
                }
                case Windowing::WindowAggregationDescriptor::Type::None: {
                    throw NotImplemented();
                    break;
                }
            }
        }
        else
        {
            throw NotImplemented("Currently complex expression in as fields are not supported");
        }
    }
    return aggregationFunctions;
}

std::shared_ptr<Runtime::Execution::Operators::WindowBasedOperatorHandler> PhysicalWindowOperator::getOperatorHandler() const
{
    return windowHandler;
}

std::ostream& PhysicalWindowOperator::toDebugString(std::ostream& os) const
{
    os << "\nPhysicalWindowOperator\n";
    return PhysicalUnaryOperator::toDebugString(os);
}

}

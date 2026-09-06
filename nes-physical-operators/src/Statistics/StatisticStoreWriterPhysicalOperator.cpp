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

#include <Statistics/StatisticStoreWriterPhysicalOperator.hpp>

#include <DataTypes/DataType.hpp>

#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Identifiers/StatisticIdentifiers.hpp>
#include <Interface/Record.hpp>
#include <Operators/Statistic/StatisticBlobType.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <StatisticStore/AbstractStatisticStore.hpp>
#include <Statistics/StatisticStoreOperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <nautilus/function.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <StatisticTuple.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{

void insertStatisticIntoStoreProxy(
    OperatorHandler* ptrOpHandler,
    const uint64_t statisticId,
    const int8_t* typeNamePtr,
    const uint64_t typeNameSize,
    const uint64_t startTs,
    const uint64_t endTs,
    const uint64_t numberOfSeenMeasurements,
    const int8_t* data,
    const uint64_t statisticDataSize)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
    PRECONDITION(data != nullptr, "The statistic data pointer must not be null");

    /// The blob lives in arena memory owned by the emitting pipeline, so it must be copied into the store.
    /// NOLINTNEXTLINE(modernize-avoid-c-arrays) dynamic byte buffer requires array form
    auto statisticData = std::make_shared<std::byte[]>(statisticDataSize);
    std::memcpy(statisticData.get(), data, statisticDataSize);

    auto* opHandler = dynamic_cast<StatisticStoreOperatorHandler*>(ptrOpHandler);
    opHandler->getStore().insertStatistic(
        StatisticId(statisticId),
        StatisticTuple{
            StatisticId(statisticId),
            StatisticBlobType{std::string_view(reinterpret_cast<const char*>(typeNamePtr), typeNameSize)},
            Timestamp(startTs),
            Timestamp(endTs),
            numberOfSeenMeasurements,
            std::move(statisticData),
            statisticDataSize});
}

}

StatisticStoreWriterPhysicalOperator::StatisticStoreWriterPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    const StatisticId statisticId,
    StatisticBlobType typeName,
    FieldIdentifiers fieldIdentifiers,
    DataType payloadType)
    : operatorHandlerId(operatorHandlerId)
    , statisticId(statisticId)
    , typeName(std::move(typeName))
    , fieldIdentifiers(std::move(fieldIdentifiers))
    , payloadType(std::move(payloadType))
{
}

void StatisticStoreWriterPhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    const auto startTs = record.read(fieldIdentifiers.inputStatisticStart).getRawValueAs<nautilus::val<uint64_t>>();
    const auto endTs = record.read(fieldIdentifiers.inputStatisticEnd).getRawValueAs<nautilus::val<uint64_t>>();
    const auto numberOfSeenMeasurements
        = record.read(fieldIdentifiers.inputNumberOfSeenMeasurements).getRawValueAs<nautilus::val<uint64_t>>();
    const auto& payloadValue = record.read(fieldIdentifiers.inputStatisticData);
    nautilus::val<const int8_t*> payloadPtr{nullptr};
    nautilus::val<uint64_t> payloadSize{0};
    if (payloadType.type == DataType::Type::VARSIZED)
    {
        const auto statisticData = payloadValue.getRawValueAs<VariableSizedData>();
        payloadPtr = statisticData.getContent();
        payloadSize = statisticData.getSize();
    }
    else
    {
        payloadSize = nautilus::val<uint64_t>{payloadType.getSizeInBytesWithoutNull()};
        const auto payloadMemory = executionCtx.pipelineMemoryProvider.arena.allocateMemory(payloadSize);
        payloadValue.writeToMemory(payloadMemory);
        payloadPtr = payloadMemory;
    }

    invoke(
        insertStatisticIntoStoreProxy,
        executionCtx.getGlobalOperatorHandler(operatorHandlerId),
        nautilus::val<uint64_t>{statisticId.getRawValue()},
        nautilus::val<const int8_t*>{reinterpret_cast<const int8_t*>(typeName.view().data())},
        nautilus::val<uint64_t>{typeName.view().size()},
        startTs,
        endTs,
        numberOfSeenMeasurements,
        payloadPtr,
        payloadSize);

    /// Re-emit the statistic metadata (without the blob) to the next operator
    Record outputRecord;
    outputRecord.write(fieldIdentifiers.outputStatisticId, VarVal{nautilus::val<uint64_t>{statisticId.getRawValue()}});
    outputRecord.write(fieldIdentifiers.outputStatisticStart, VarVal{startTs});
    outputRecord.write(fieldIdentifiers.outputStatisticEnd, VarVal{endTs});
    outputRecord.write(fieldIdentifiers.outputNumberOfSeenMeasurements, VarVal{numberOfSeenMeasurements});
    executeChild(executionCtx, outputRecord);
}

std::optional<PhysicalOperator> StatisticStoreWriterPhysicalOperator::getChild() const
{
    return child;
}

void StatisticStoreWriterPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}

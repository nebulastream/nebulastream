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

#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/Record.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Statistics/StatisticStoreOperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <nautilus/function.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <Statistic.hpp>
#include <val.hpp>

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
    PRECONDITION(ptrOpHandler != nullptr, "The operator handler must not be null");
    PRECONDITION(data != nullptr, "The statistic data pointer must not be null");

    auto* opHandler = dynamic_cast<StatisticStoreOperatorHandler*>(ptrOpHandler);
    INVARIANT(opHandler != nullptr, "The operator handler must be a StatisticStoreOperatorHandler");

    /// The blob lives in arena memory owned by the emitting pipeline, so it must be copied into the store.
    auto statisticData = std::make_shared<std::byte[]>(statisticDataSize);
    std::memcpy(statisticData.get(), data, statisticDataSize);

    opHandler->getStatisticStore()->insertStatistic(
        StatisticId(statisticId),
        Statistic{
            StatisticId(statisticId),
            std::string(reinterpret_cast<const char*>(typeNamePtr), typeNameSize),
            Timestamp(startTs),
            Timestamp(endTs),
            numberOfSeenMeasurements,
            std::move(statisticData),
            statisticDataSize});
}

}

StatisticStoreWriterPhysicalOperator::StatisticStoreWriterPhysicalOperator(
    const OperatorHandlerId operatorHandlerId, const StatisticId statisticId, std::string typeName, FieldIdentifiers fieldIdentifiers)
    : operatorHandlerId(operatorHandlerId)
    , statisticId(statisticId)
    , typeName(std::move(typeName))
    , fieldIdentifiers(std::move(fieldIdentifiers))
{
}

void StatisticStoreWriterPhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    const auto operatorHandlerRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto startTs = record.read(fieldIdentifiers.inputStatisticStart).getRawValueAs<nautilus::val<uint64_t>>();
    const auto endTs = record.read(fieldIdentifiers.inputStatisticEnd).getRawValueAs<nautilus::val<uint64_t>>();
    const auto numberOfSeenMeasurements
        = record.read(fieldIdentifiers.inputNumberOfSeenMeasurements).getRawValueAs<nautilus::val<uint64_t>>();
    const auto statisticData = record.read(fieldIdentifiers.inputStatisticData).getRawValueAs<VariableSizedData>();

    invoke(
        insertStatisticIntoStoreProxy,
        operatorHandlerRef,
        nautilus::val<uint64_t>{statisticId.getRawValue()},
        nautilus::val<const int8_t*>{reinterpret_cast<const int8_t*>(typeName.c_str())},
        nautilus::val<uint64_t>{typeName.size()},
        startTs,
        endTs,
        numberOfSeenMeasurements,
        statisticData.getContent(),
        statisticData.getSize());

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

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

#include <Statistics/StatisticStoreReaderPhysicalOperator.hpp>

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include <DataTypes/VarVal.hpp>
#include <Interface/Record.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Statistics/ReservoirSampleBlob.hpp>
#include <Statistics/StatisticStoreOperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <nautilus/function.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <Statistic.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{

StatisticStoreOperatorHandler* getStatisticStoreOperatorHandler(OperatorHandler* ptrOpHandler)
{
    PRECONDITION(ptrOpHandler != nullptr, "The operator handler must not be null");
    auto* opHandler = dynamic_cast<StatisticStoreOperatorHandler*>(ptrOpHandler);
    INVARIANT(opHandler != nullptr, "The operator handler must be a StatisticStoreOperatorHandler");
    return opHandler;
}

/// Returns the statistic blob for (statisticId, startTs, endTs), or nullptr if no such statistic exists.
/// The returned pointer stays valid because the store shares ownership of the blob and entries are never deleted
/// while queries run.
int8_t* getStatisticDataProxy(OperatorHandler* ptrOpHandler, const uint64_t statisticId, const uint64_t startTs, const uint64_t endTs)
{
    const auto* opHandler = getStatisticStoreOperatorHandler(ptrOpHandler);
    const auto statistic
        = opHandler->getStatisticStore()->getSingleStatistic(StatisticId(statisticId), Timestamp(startTs), Timestamp(endTs));
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast): the reader only reads; nautilus record access needs int8_t*.
    return statistic.has_value() ? const_cast<int8_t*>(statistic->getStatisticData()) : nullptr;
}

uint64_t getNumberOfSeenTuplesProxy(OperatorHandler* ptrOpHandler, const uint64_t statisticId, const uint64_t startTs, const uint64_t endTs)
{
    const auto* opHandler = getStatisticStoreOperatorHandler(ptrOpHandler);
    const auto statistic
        = opHandler->getStatisticStore()->getSingleStatistic(StatisticId(statisticId), Timestamp(startTs), Timestamp(endTs));
    return statistic.has_value() ? statistic->getNumberOfSeenMeasurements() : 0;
}

}

StatisticStoreReaderPhysicalOperator::StatisticStoreReaderPhysicalOperator(
    const OperatorHandlerId operatorHandlerId, FieldIdentifiers fieldIdentifiers, std::vector<ReservoirSampleField> sampleFields)
    : operatorHandlerId(operatorHandlerId), fieldIdentifiers(std::move(fieldIdentifiers)), sampleFields(std::move(sampleFields))
{
}

void StatisticStoreReaderPhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    const auto operatorHandlerRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto statisticId = record.read(fieldIdentifiers.inputStatisticId).getRawValueAs<nautilus::val<uint64_t>>();
    const auto startTs = record.read(fieldIdentifiers.inputStatisticStart).getRawValueAs<nautilus::val<uint64_t>>();
    const auto endTs = record.read(fieldIdentifiers.inputStatisticEnd).getRawValueAs<nautilus::val<uint64_t>>();

    const auto numberOfSeenTuples = invoke(getNumberOfSeenTuplesProxy, operatorHandlerRef, statisticId, startTs, endTs);
    const auto blobMemArea = invoke(getStatisticDataProxy, operatorHandlerRef, statisticId, startTs, endTs);
    if (blobMemArea != nullptr)
    {
        const auto tupleCount = readReservoirSampleBlobTupleCount(blobMemArea);
        ReservoirSampleBlobRowReader rowReader(blobMemArea, sampleFields);
        for (nautilus::val<uint64_t> i = 0; i < tupleCount; i = i + 1)
        {
            Record sampleRecord = rowReader.readNextTuple();
            sampleRecord.write(fieldIdentifiers.outputStatisticStart, VarVal{startTs});
            sampleRecord.write(fieldIdentifiers.outputStatisticEnd, VarVal{endTs});
            sampleRecord.write(fieldIdentifiers.outputNumberOfSeenTuples, VarVal{numberOfSeenTuples});
            executeChild(executionCtx, sampleRecord);
        }
    }
}

std::optional<PhysicalOperator> StatisticStoreReaderPhysicalOperator::getChild() const
{
    return child;
}

void StatisticStoreReaderPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}

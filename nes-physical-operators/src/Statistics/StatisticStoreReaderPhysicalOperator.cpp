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
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/VarVal.hpp>
#include <Identifiers/StatisticIdentifiers.hpp>
#include <Interface/NESStrongTypeRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/TimestampRef.hpp>
#include <Operators/Statistic/StatisticBlobType.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <StatisticStore/AbstractStatisticStore.hpp>
#include <Statistics/StatisticIterator.hpp>
#include <Statistics/StatisticStoreOperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <nautilus/function.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <StatisticTuple.hpp>
#include <val_arith.hpp>
#include <val_enum.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{

thread_local std::unordered_map<uint64_t, std::vector<AbstractStatisticStore::StatisticRef>> tProbeStatistics;

/// A probe declares what it expects to read, but the store is keyed by statisticId alone, so a probe can reach a
/// statistic some other build wrote. Reading it anyway would reinterpret the stored bytes -- a FLOAT64 average read
/// as a UINT64 count -- or read past a shorter payload. Fail loudly instead.
void validateAgainstProbe(
    const std::vector<AbstractStatisticStore::StatisticRef>& statistics,
    const StatisticId statisticId,
    const StatisticBlobType& expectedTypeName,
    const uint64_t expectedPayloadSizeInBytes)
{
    for (const auto& statistic : statistics)
    {
        if (statistic->getTypeName() != expectedTypeName)
        {
            throw CannotProbeStatistic(
                "StatisticTuple {} was built as {} but is probed as {}",
                statisticId.getRawValue(),
                statistic->getTypeName().getRawValue(),
                expectedTypeName.getRawValue());
        }
        if (expectedPayloadSizeInBytes != 0 and statistic->getStatisticDataSize() != expectedPayloadSizeInBytes)
        {
            throw CannotProbeStatistic(
                "StatisticTuple {} persisted a {}-byte payload but is probed as a {}-byte type",
                statisticId.getRawValue(),
                statistic->getStatisticDataSize(),
                expectedPayloadSizeInBytes);
        }
    }
}

uint64_t loadStatisticsProxy(
    OperatorHandler* ptrOpHandler,
    const uint64_t readerId,
    const StatisticId statisticId,
    const Timestamp startTs,
    const Timestamp endTs,
    const StatisticIterator* statisticIterator,
    const StatisticWindowMatch windowMatch)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
    PRECONDITION(statisticIterator != nullptr, "the statistic iterator should not be null!");

    auto& store = dynamic_cast<StatisticStoreOperatorHandler*>(ptrOpHandler)->getStore();
    std::vector<AbstractStatisticStore::StatisticRef> statistics;
    if (windowMatch == StatisticWindowMatch::ExactWindow)
    {
        if (auto statistic = store.getSingleStatistic(statisticId, Timestamp{startTs.getRawValue()}, Timestamp{endTs.getRawValue()}))
        {
            statistics.push_back(std::move(statistic.value()));
        }
    }
    else
    {
        statistics = store.getStatistics(statisticId, Timestamp{startTs.getRawValue()}, Timestamp{endTs.getRawValue()});
    }

    validateAgainstProbe(
        statistics, statisticId, statisticIterator->getStatisticBlobType(), statisticIterator->getExpectedPayloadSizeInBytes());
    auto& frame = tProbeStatistics[readerId];
    frame = std::move(statistics);
    return frame.size();
}

void releaseStatisticsProxy(const uint64_t readerId)
{
    tProbeStatistics[readerId].clear();
}

int8_t* getStatisticDataByIndexProxy(const uint64_t readerId, const uint64_t index)
{
    const auto& statistics = tProbeStatistics[readerId];
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast): the reader only reads; nautilus record access needs int8_t*.
    return index < statistics.size() ? const_cast<int8_t*>(statistics[index]->getStatisticData()) : nullptr;
}

uint64_t getSeenTuplesByIndexProxy(const uint64_t readerId, const uint64_t index)
{
    const auto& statistics = tProbeStatistics[readerId];
    return index < statistics.size() ? statistics[index]->getNumberOfSeenMeasurements() : 0;
}

uint64_t getStartTsByIndexProxy(const uint64_t readerId, const uint64_t index)
{
    const auto& statistics = tProbeStatistics[readerId];
    return index < statistics.size() ? statistics[index]->getStartTs().getRawValue() : 0;
}

uint64_t getEndTsByIndexProxy(const uint64_t readerId, const uint64_t index)
{
    const auto& statistics = tProbeStatistics[readerId];
    return index < statistics.size() ? statistics[index]->getEndTs().getRawValue() : 0;
}

}

StatisticStoreReaderPhysicalOperator::StatisticStoreReaderPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    const StatisticId statisticId,
    FieldIdentifiers fieldIdentifiers,
    std::shared_ptr<StatisticIterator> statisticIterator,
    const StatisticWindowMatch windowMatch)
    : operatorHandlerId(operatorHandlerId)
    , statisticId(statisticId)
    , fieldIdentifiers(std::move(fieldIdentifiers))
    , statisticIterator(std::move(statisticIterator))
    , windowMatch(windowMatch)
{
}

void StatisticStoreReaderPhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    /// Only the probed window comes from the record; which statistic to read is operator state.
    const nautilus::val<StatisticId> statisticIdVal{statisticId};
    const nautilus::val<Timestamp> startTs{
        record.read(fieldIdentifiers.inputStatisticStart).getRawValueAs<nautilus::val<Timestamp::Underlying>>()};
    const nautilus::val<Timestamp> endTs{
        record.read(fieldIdentifiers.inputStatisticEnd).getRawValueAs<nautilus::val<Timestamp::Underlying>>()};
    const nautilus::val<const StatisticIterator*> iteratorRef{statisticIterator.get()};
    const nautilus::val<StatisticWindowMatch> windowMatchVal{windowMatch};
    const nautilus::val<uint64_t> readerId{operatorHandlerId.getRawValue()};

    const auto statisticCount = invoke(
        loadStatisticsProxy,
        executionCtx.getGlobalOperatorHandler(operatorHandlerId),
        readerId,
        statisticIdVal,
        startTs,
        endTs,
        iteratorRef,
        windowMatchVal);

    for (nautilus::val<uint64_t> i = 0; i < statisticCount; i = i + 1)
    {
        const auto payload = invoke(getStatisticDataByIndexProxy, readerId, i);
        if (payload != nullptr)
        {
            /// The window bounds reported are the stored statistic's own, not the probed range, so a caller that
            /// asked over a span learns which window each row actually belongs to.
            const auto statisticStart = invoke(getStartTsByIndexProxy, readerId, i);
            const auto statisticEnd = invoke(getEndTsByIndexProxy, readerId, i);
            const auto numberOfSeenTuples = invoke(getSeenTuplesByIndexProxy, readerId, i);

            statisticIterator->forEachRecord(
                payload,
                [&](Record& statisticRecord)
                {
                    statisticRecord.write(fieldIdentifiers.outputStatisticId, statisticIdVal.convertToValue());
                    statisticRecord.write(fieldIdentifiers.outputStatisticStart, VarVal{statisticStart});
                    statisticRecord.write(fieldIdentifiers.outputStatisticEnd, VarVal{statisticEnd});
                    statisticRecord.write(fieldIdentifiers.outputNumberOfSeenTuples, VarVal{numberOfSeenTuples});
                    executeChild(executionCtx, statisticRecord);
                });
        }
    }

    invoke(releaseStatisticsProxy, readerId);
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

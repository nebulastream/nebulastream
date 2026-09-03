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

#include <Statistics/StatisticStoreReader.hpp>

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/NESStrongTypeRef.hpp>
#include <Interface/Record.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <StatisticTuple.hpp>
#include <Statistics/StatisticStoreOperatorHandler.hpp>
#include <Statistic/StatisticTypes.hpp>
#include <StatisticStore/AbstractStatisticStore.hpp>
#include <Time/Timestamp.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_enum.hpp>

namespace NES
{

namespace
{

/// Holds the statistics matching a single execute() call. Safe as thread-local because a worker thread runs an
/// operator pipeline without re-entering execute().
thread_local std::vector<StatisticTuple> tProbeStatistics;

/// A probe declares what it expects to read, but the store is keyed by statisticId alone, so a probe can reach a
/// statistic some other build wrote. Reading it anyway would reinterpret the stored bytes -- a FLOAT64 average read
/// as a UINT64 count -- or read past a shorter payload. Fail loudly instead.
void validateAgainstProbe(
    const std::vector<StatisticTuple>& statistics,
    const StatisticId statisticId,
    const StatisticType expectedType,
    const uint64_t expectedPayloadSizeInBytes)
{
    for (const auto& statistic : statistics)
    {
        if (statistic.getTypeName() != magic_enum::enum_name(expectedType))
        {
            throw CannotProbeStatistic(
                "StatisticTuple {} was built as {} but is probed as {}",
                statisticId.getRawValue(),
                statistic.getTypeName(),
                magic_enum::enum_name(expectedType));
        }
        if (statistic.getStatisticDataSize() != expectedPayloadSizeInBytes)
        {
            throw CannotProbeStatistic(
                "StatisticTuple {} persisted a {}-byte payload but is probed as a {}-byte type",
                statisticId.getRawValue(),
                statistic.getStatisticDataSize(),
                expectedPayloadSizeInBytes);
        }
    }
}

uint64_t loadStatisticsProxy(
    OperatorHandler* ptrOpHandler,
    const StatisticId statisticId,
    const Timestamp startTs,
    const Timestamp endTs,
    const StatisticType expectedType,
    const uint64_t expectedPayloadSizeInBytes)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null!");
    const auto* opHandler = dynamic_cast<StatisticStoreOperatorHandler*>(ptrOpHandler);
    PRECONDITION(opHandler != nullptr, "opHandler should be a StatisticStoreOperatorHandler!");

    tProbeStatistics = opHandler->getStatisticStore()->getStatistics(
        statisticId, Timestamp{startTs.getRawValue()}, Timestamp{endTs.getRawValue()});

    validateAgainstProbe(tProbeStatistics, statisticId, expectedType, expectedPayloadSizeInBytes);
    return tProbeStatistics.size();
}

const int8_t* getStatisticDataByIndexProxy(const uint64_t index)
{
    return index < tProbeStatistics.size() ? tProbeStatistics[index].getStatisticData() : nullptr;
}

uint64_t getSeenTuplesByIndexProxy(const uint64_t index)
{
    return index < tProbeStatistics.size() ? tProbeStatistics[index].getNumberOfSeenMeasurements() : 0;
}

uint64_t getStartTsByIndexProxy(const uint64_t index)
{
    return index < tProbeStatistics.size() ? tProbeStatistics[index].getStartTs().getRawValue() : 0;
}

uint64_t getEndTsByIndexProxy(const uint64_t index)
{
    return index < tProbeStatistics.size() ? tProbeStatistics[index].getEndTs().getRawValue() : 0;
}

}

StatisticStoreReader::StatisticStoreReader(
    const OperatorHandlerId operatorHandlerId,
    const StatisticId statisticId,
    const StatisticType expectedType,
    DataType valueType,
    Record::RecordFieldIdentifier inputStartTsFieldName,
    Record::RecordFieldIdentifier inputEndTsFieldName,
    Record::RecordFieldIdentifier outputStatisticIdFieldName,
    Record::RecordFieldIdentifier outputStartTsFieldName,
    Record::RecordFieldIdentifier outputEndTsFieldName,
    Record::RecordFieldIdentifier outputNumberOfSeenTuplesFieldName,
    Record::RecordFieldIdentifier outputValueFieldName)
    : operatorHandlerId(operatorHandlerId)
    , statisticId(statisticId)
    , expectedType(expectedType)
    , valueType(std::move(valueType))
    , inputStartTsFieldName(std::move(inputStartTsFieldName))
    , inputEndTsFieldName(std::move(inputEndTsFieldName))
    , outputStatisticIdFieldName(std::move(outputStatisticIdFieldName))
    , outputStartTsFieldName(std::move(outputStartTsFieldName))
    , outputEndTsFieldName(std::move(outputEndTsFieldName))
    , outputNumberOfSeenTuplesFieldName(std::move(outputNumberOfSeenTuplesFieldName))
    , outputValueFieldName(std::move(outputValueFieldName))
{
}

void StatisticStoreReader::execute(ExecutionContext& executionCtx, Record& record) const
{
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);

    /// Only the probed window comes from the record; which statistic to read is operator state.
    const nautilus::val<StatisticId> statisticIdVal{statisticId};
    const nautilus::val<StatisticType> expectedTypeVal{expectedType};
    const nautilus::val<uint64_t> expectedPayloadSize{valueType.getSizeInBytesWithoutNull()};
    const nautilus::val<Timestamp> startTs{record.read(inputStartTsFieldName).getRawValueAs<nautilus::val<Timestamp::Underlying>>()};
    const nautilus::val<Timestamp> endTs{record.read(inputEndTsFieldName).getRawValueAs<nautilus::val<Timestamp::Underlying>>()};

    const auto statisticCount
        = invoke(loadStatisticsProxy, operatorHandlerMemRef, statisticIdVal, startTs, endTs, expectedTypeVal, expectedPayloadSize);

    for (nautilus::val<uint64_t> i = 0; i < statisticCount; ++i)
    {
        const auto statisticMemArea = invoke(getStatisticDataByIndexProxy, i);
        if (statisticMemArea != nullptr)
        {
            /// The value is the whole payload, so it sits at offset 0 with no header to skip.
            Record statisticRecord;
            statisticRecord.write(outputValueFieldName, VarVal::readNonNullableVarValFromMemory(statisticMemArea, valueType));

            /// The window bounds reported are the stored statistic's own, not the probed range, so a caller that
            /// asked over a span learns which window each row actually belongs to.
            statisticRecord.write(outputStatisticIdFieldName, statisticIdVal.convertToValue());
            statisticRecord.write(outputStartTsFieldName, invoke(getStartTsByIndexProxy, i));
            statisticRecord.write(outputEndTsFieldName, invoke(getEndTsByIndexProxy, i));
            statisticRecord.write(outputNumberOfSeenTuplesFieldName, invoke(getSeenTuplesByIndexProxy, i));
            executeChild(executionCtx, statisticRecord);
        }
    }
}

std::optional<PhysicalOperator> StatisticStoreReader::getChild() const
{
    return child;
}

void StatisticStoreReader::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}

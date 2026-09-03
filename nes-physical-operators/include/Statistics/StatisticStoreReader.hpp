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

#pragma once

#include <optional>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <Interface/Record.hpp>
#include <PhysicalOperator.hpp>
#include <Statistic/StatisticTypes.hpp>

namespace NES
{

/// Reads scalar statistics back out of the statistic store, emitting one record per stored window in the probed
/// range.
///
/// This is the only reader of the store. Unlike the branch this is ported from there is no StatisticProvider
/// indirection: that layer exists to dispatch between four payload layouts (histogram bins, sketch rows, sample
/// entries, scalar), and with only the scalar shape ported it would be an interface, an iterator hierarchy and an
/// argument-cloning protocol wrapping a single read at offset 0. If the synopses are ever ported, that abstraction
/// has to come back with them.
///
/// The statisticId and the expected type are operator state, not record fields: a probe is built for one statistic.
/// Only the window bounds come from the incoming record, which is what lets the same operator serve both a probe
/// chained directly after the build and one driven by impulse tuples.
class StatisticStoreReader final : public PhysicalOperatorConcept
{
public:
    StatisticStoreReader(
        OperatorHandlerId operatorHandlerId,
        StatisticId statisticId,
        StatisticType expectedType,
        DataType valueType,
        Record::RecordFieldIdentifier inputStartTsFieldName,
        Record::RecordFieldIdentifier inputEndTsFieldName,
        Record::RecordFieldIdentifier outputStatisticIdFieldName,
        Record::RecordFieldIdentifier outputStartTsFieldName,
        Record::RecordFieldIdentifier outputEndTsFieldName,
        Record::RecordFieldIdentifier outputNumberOfSeenTuplesFieldName,
        Record::RecordFieldIdentifier outputValueFieldName);

    void execute(ExecutionContext& executionCtx, Record& record) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    std::optional<PhysicalOperator> child;
    OperatorHandlerId operatorHandlerId;
    StatisticId statisticId;
    StatisticType expectedType;
    DataType valueType;
    Record::RecordFieldIdentifier inputStartTsFieldName;
    Record::RecordFieldIdentifier inputEndTsFieldName;
    Record::RecordFieldIdentifier outputStatisticIdFieldName;
    Record::RecordFieldIdentifier outputStartTsFieldName;
    Record::RecordFieldIdentifier outputEndTsFieldName;
    Record::RecordFieldIdentifier outputNumberOfSeenTuplesFieldName;
    Record::RecordFieldIdentifier outputValueFieldName;
};

}

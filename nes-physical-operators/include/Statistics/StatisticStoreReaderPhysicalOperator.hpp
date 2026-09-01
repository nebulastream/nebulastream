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
#include <vector>
#include <Interface/Record.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Statistics/ReservoirSampleBlob.hpp>
#include <Statistics/Statistic.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

/// For every input record (statistic metadata), looks up the reservoir sample blob for
/// (statisticId, statisticStart, statisticEnd) in the worker's statistic store and emits one record per sampled
/// tuple, decoded according to the declared sample fields and enriched with the statistic metadata.
class StatisticStoreReaderPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    struct FieldIdentifiers
    {
        Record::RecordFieldIdentifier inputStatisticId;
        Record::RecordFieldIdentifier inputStatisticStart;
        Record::RecordFieldIdentifier inputStatisticEnd;
        Record::RecordFieldIdentifier outputStatisticStart;
        Record::RecordFieldIdentifier outputStatisticEnd;
        Record::RecordFieldIdentifier outputNumberOfSeenTuples;
    };

    StatisticStoreReaderPhysicalOperator(
        OperatorHandlerId operatorHandlerId, FieldIdentifiers fieldIdentifiers, std::vector<ReservoirSampleField> sampleFields);

    void execute(ExecutionContext& executionCtx, Record& record) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    OperatorHandlerId operatorHandlerId;
    FieldIdentifiers fieldIdentifiers;
    std::vector<ReservoirSampleField> sampleFields;

    std::optional<PhysicalOperator> child;
};

}

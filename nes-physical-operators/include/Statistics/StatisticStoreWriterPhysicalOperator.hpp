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
#include <string>
#include <Interface/Record.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Identifiers/StatisticIdentifiers.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

/// Writes the statistic blob built by the upstream statistic build into the worker's statistic store and re-emits
/// the statistic metadata record [statisticId, statisticStart, statisticEnd, statisticNumberOfSeenMeasurements].
class StatisticStoreWriterPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    struct FieldIdentifiers
    {
        Record::RecordFieldIdentifier inputStatisticStart;
        Record::RecordFieldIdentifier inputStatisticEnd;
        Record::RecordFieldIdentifier inputStatisticData;
        Record::RecordFieldIdentifier inputNumberOfSeenMeasurements;
        Record::RecordFieldIdentifier outputStatisticId;
        Record::RecordFieldIdentifier outputStatisticStart;
        Record::RecordFieldIdentifier outputStatisticEnd;
        Record::RecordFieldIdentifier outputNumberOfSeenMeasurements;
    };

    StatisticStoreWriterPhysicalOperator(
        OperatorHandlerId operatorHandlerId, StatisticId statisticId, std::string typeName, FieldIdentifiers fieldIdentifiers);

    void execute(ExecutionContext& executionCtx, Record& record) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    OperatorHandlerId operatorHandlerId;
    StatisticId statisticId;
    std::string typeName;
    FieldIdentifiers fieldIdentifiers;

    std::optional<PhysicalOperator> child;
};

}

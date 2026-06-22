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

#include <Statistic/StatisticStore/StatisticStoreReader.hpp>

#include <cstdint>
#include <optional>
#include <utility>

#include <DataTypes/VarVal.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/Record.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <nautilus/val.hpp>

#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <Statistic.hpp>
#include <Statistic/StatisticStore/StatisticStoreOperatorHandler.hpp>
#include <StatisticStore/AbstractStatisticStore.hpp>
#include <function.hpp>

namespace NES
{

StatisticStoreReader::StatisticStoreReader(
    OperatorHandlerId operatorHandlerId,
    Record::RecordFieldIdentifier statisticIdField,
    Record::RecordFieldIdentifier startField,
    Record::RecordFieldIdentifier endField,
    Record::RecordFieldIdentifier valueField)
    : operatorHandlerId(operatorHandlerId)
    , statisticIdField(std::move(statisticIdField))
    , startField(std::move(startField))
    , endField(std::move(endField))
    , valueField(std::move(valueField))
{
}

void StatisticStoreReader::execute(ExecutionContext& ctx, Record& record) const
{
    const auto statisticId = record.read(statisticIdField).getRawValueAs<nautilus::val<uint64_t>>();
    const auto startTs = record.read(startField).getRawValueAs<nautilus::val<uint64_t>>();
    const auto endTs = record.read(endField).getRawValueAs<nautilus::val<uint64_t>>();

    const auto value = nautilus::invoke(
        +[](OperatorHandler* handler, uint64_t statisticId, uint64_t startTs, uint64_t endTs) -> uint64_t
        {
            PRECONDITION(handler != nullptr, "StatisticStoreReader expects a valid operator handler");
            const auto store = dynamic_cast<StatisticStoreOperatorHandler&>(*handler).getStatisticStore();
            const auto statistic = store->getSingleStatistic(
                Statistic::StatisticId{statisticId}, Windowing::TimeMeasure{startTs}, Windowing::TimeMeasure{endTs});
            return statistic.has_value() ? statistic->getNumberOfSeenTuples() : 0;
        },
        ctx.getGlobalOperatorHandler(operatorHandlerId),
        statisticId,
        startTs,
        endTs);

    record.write(valueField, VarVal{value});

    executeChild(ctx, record);
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

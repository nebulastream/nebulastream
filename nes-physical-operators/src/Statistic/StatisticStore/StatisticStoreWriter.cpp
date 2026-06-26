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

#include <Statistic/StatisticStore/StatisticStoreWriter.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
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

StatisticStoreWriter::StatisticStoreWriter(
    OperatorHandlerId operatorHandlerId,
    uint64_t statisticId,
    Record::RecordFieldIdentifier startField,
    Record::RecordFieldIdentifier endField,
    Record::RecordFieldIdentifier valueField)
    : operatorHandlerId(operatorHandlerId)
    , statisticId(statisticId)
    , startField(std::move(startField))
    , endField(std::move(endField))
    , valueField(std::move(valueField))
{
}

void StatisticStoreWriter::execute(ExecutionContext& ctx, Record& record) const
{
    const auto startTs = record.read(startField).getRawValueAs<nautilus::val<uint64_t>>();
    const auto endTs = record.read(endField).getRawValueAs<nautilus::val<uint64_t>>();
    const auto value = record.read(valueField).getRawValueAs<nautilus::val<uint64_t>>();

    nautilus::invoke(
        +[](OperatorHandler* handler, uint64_t statisticId, uint64_t startTs, uint64_t endTs, uint64_t value)
        {
            PRECONDITION(handler != nullptr, "StatisticStoreWriter expects a valid operator handler");
            const auto store = dynamic_cast<StatisticStoreOperatorHandler&>(*handler).getStatisticStore();
            Statistic statistic{
                Statistic::StatisticId{statisticId},
                Statistic::StatisticType::Equi_Width_Histogram,
                Windowing::TimeMeasure{startTs},
                Windowing::TimeMeasure{endTs},
                value,
                std::shared_ptr<std::byte[]>{},
                0};
            store->insertStatistic(Statistic::StatisticId{statisticId}, std::move(statistic));
        },
        ctx.getGlobalOperatorHandler(operatorHandlerId),
        nautilus::val<uint64_t>(statisticId),
        startTs,
        endTs,
        value);

    executeChild(ctx, record);
}

std::optional<PhysicalOperator> StatisticStoreWriter::getChild() const
{
    return child;
}

void StatisticStoreWriter::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}

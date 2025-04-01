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

#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Statistic/StatisticStoreOperatorHandler.hpp>
#include <Execution/Operators/Statistic/StatisticStoreReader.hpp>

namespace NES::Runtime::Execution::Operators
{

// TODO(nikla44): what if I don't want to get a single statistic but all in the given bounds?

int8_t* getStatisticFromStoreProxy(OperatorHandler* ptrOpHandler, const StatisticHash hash, const Timestamp startTs, const Timestamp endTs)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null!");

    const auto* opHandler = dynamic_cast<StatisticStoreOperatorHandler*>(ptrOpHandler);
    const auto statisticStore = opHandler->getStatisticStore();

    const auto statistic = statisticStore->getSingleStatistic(
        hash, Windowing::TimeMeasure(startTs.getRawValue()), Windowing::TimeMeasure(endTs.getRawValue()));

    if (statistic.has_value())
    {
        return statistic.value()->getStatisticData();
    }
    return nullptr;
}

StatisticStoreReader::StatisticStoreReader(const uint64_t operatorHandlerIndex) : operatorHandlerIndex(operatorHandlerIndex)
{
}

void StatisticStoreReader::execute(ExecutionContext& executionCtx, Record& record) const
{
    /// Insert statistic into store
    // TODO(nikla44): should recordFieldIdentifiers be hardcoded? What about the statisticType field?
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    const auto statisticHash = record.read("hash").cast<nautilus::val<StatisticHash>>();
    const auto startTs = record.read("startTs").cast<nautilus::val<Timestamp>>();
    const auto endTs = record.read("endTs").cast<nautilus::val<Timestamp>>();
    const auto statisticData = invoke(getStatisticFromStoreProxy, operatorHandlerMemRef, statisticHash, startTs, endTs);
    record.write("data", VariableSizedData(statisticData));
}

}

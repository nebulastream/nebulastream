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

/// For now, we cannot call getAllStatistics() from StatisticStore as we can only pass one value back to Nautilus
int8_t* getStatisticFromStoreProxy(
    OperatorHandler* ptrOpHandler, const StatisticHash hash, const Timestamp::Underlying startTs, const Timestamp::Underlying endTs)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null!");

    const auto* opHandler = dynamic_cast<StatisticStoreOperatorHandler*>(ptrOpHandler);
    const auto statisticStore = opHandler->getStatisticStore();

    const auto statistic = statisticStore->getSingleStatistic(hash, Windowing::TimeMeasure(startTs), Windowing::TimeMeasure(endTs));
    if (statistic.has_value())
    {
        return statistic.value()->getStatisticData();
    }
    return nullptr;
}

StatisticStoreReader::StatisticStoreReader(
    const uint64_t operatorHandlerIndex,
    const std::string& hashFieldName,
    const std::string& startTsFieldName,
    const std::string& endTsFieldName,
    const std::string& dataFieldName)
    : operatorHandlerIndex(operatorHandlerIndex)
    , hashFieldName(hashFieldName)
    , startTsFieldName(startTsFieldName)
    , endTsFieldName(endTsFieldName)
    , dataFieldName(dataFieldName)
{
}

void StatisticStoreReader::execute(ExecutionContext& executionCtx, Record& record) const
{
    /// Read statistic from store
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    const auto statisticHash = record.read(hashFieldName).cast<nautilus::val<StatisticHash>>();
    const auto startTs = record.read(startTsFieldName).cast<nautilus::val<Timestamp::Underlying>>();
    const auto endTs = record.read(endTsFieldName).cast<nautilus::val<Timestamp::Underlying>>();
    const auto statisticData = invoke(getStatisticFromStoreProxy, operatorHandlerMemRef, statisticHash, startTs, endTs);
    record.write(dataFieldName, VariableSizedData(statisticData));
}

}

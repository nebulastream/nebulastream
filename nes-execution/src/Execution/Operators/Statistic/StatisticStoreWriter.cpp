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
#include <Execution/Operators/Statistic/StatisticStoreWriter.hpp>

namespace NES::Runtime::Execution::Operators
{

void insertStatisticIntoStoreProxy(
    OperatorHandler* ptrOpHandler,
    const StatisticHash hash,
    const std::underlying_type_t<StatisticType> type,
    const Timestamp::Underlying startTs,
    const Timestamp::Underlying endTs,
    int8_t* data)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null!");
    PRECONDITION(data != nullptr, "statistic data pointer should not be null!");

    const auto* opHandler = dynamic_cast<StatisticStoreOperatorHandler*>(ptrOpHandler);
    const auto statisticStore = opHandler->getStatisticStore();

    const auto statisticDataSize = *reinterpret_cast<uint32_t*>(data) + sizeof(uint32_t);
    std::vector<int8_t> statisticData(statisticDataSize);
    std::memcpy(statisticData.data(), data, statisticDataSize);

    const auto statistic = std::make_shared<Statistic>(
        static_cast<StatisticType>(type), Windowing::TimeMeasure(startTs), Windowing::TimeMeasure(endTs), statisticData);

    statisticStore->insertStatistic(hash, statistic);
}

StatisticStoreWriter::StatisticStoreWriter(
    const uint64_t operatorHandlerIndex,
    const std::string& hashFieldName,
    const std::string& typeFieldName,
    const std::string& startTsFieldName,
    const std::string& endTsFieldName,
    const std::string& dataFieldName)
    : operatorHandlerIndex(operatorHandlerIndex)
    , hashFieldName(hashFieldName)
    , typeFieldName(typeFieldName)
    , startTsFieldName(startTsFieldName)
    , endTsFieldName(endTsFieldName)
    , dataFieldName(dataFieldName)
{
}

void StatisticStoreWriter::execute(ExecutionContext& executionCtx, Record& record) const
{
    /// Insert statistic into store
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    const auto statisticHash = record.read(hashFieldName).cast<nautilus::val<StatisticHash>>();
    const auto statisticType = record.read(typeFieldName).cast<nautilus::val<std::underlying_type_t<StatisticType>>>();
    const auto startTs = record.read(startTsFieldName).cast<nautilus::val<Timestamp::Underlying>>();
    const auto endTs = record.read(endTsFieldName).cast<nautilus::val<Timestamp::Underlying>>();
    const auto statisticData = record.read(dataFieldName).cast<VariableSizedData>().getReference();
    invoke(insertStatisticIntoStoreProxy, operatorHandlerMemRef, statisticHash, statisticType, startTs, endTs, statisticData);
}

}

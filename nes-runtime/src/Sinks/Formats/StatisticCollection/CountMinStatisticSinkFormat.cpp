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

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Synopses/CountMinStatistic.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Formats/StatisticCollection/CountMinStatisticSinkFormat.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <utility>

namespace NES::Statistic {

AbstractStatisticSinkFormatPtr CountMinStatisticSinkFormat::create(Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout) {
    const auto qualifierNameWithSeparator = memoryLayout->getSchema()->getQualifierNameForSystemGeneratedFieldsWithSeparator();
    return std::make_shared<CountMinStatisticSinkFormat>(
        CountMinStatisticSinkFormat(qualifierNameWithSeparator, std::move(memoryLayout)));
}

CountMinStatisticSinkFormat::CountMinStatisticSinkFormat(const std::string& qualifierNameWithSeparator,
                                                         Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout)
    : AbstractStatisticSinkFormat(qualifierNameWithSeparator, std::move(memoryLayout)),
      widthFieldName(qualifierNameWithSeparator + WIDTH_FIELD_NAME),
      depthFieldName(qualifierNameWithSeparator + DEPTH_FIELD_NAME),
      countMinDataFieldName(qualifierNameWithSeparator + STATISTIC_DATA_FIELD_NAME) {}

std::vector<HashStatisticPair> CountMinStatisticSinkFormat::readStatisticsFromBuffer(Runtime::TupleBuffer& buffer) {
    std::vector<HashStatisticPair> createdStatisticsWithTheirHash;

    // Each tuple represents one CountMin-Sketch
    for (auto curTupleCnt = 0_u64; curTupleCnt < buffer.getNumberOfTuples(); ++curTupleCnt) {
        // Calculating / Getting the field offset for each of the fields we need
        const auto startTsFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, startTsFieldName);
        const auto endTsFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, endTsFieldName);
        const auto hashFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, statisticHashFieldName);
        const auto observedTuplesFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, observedTuplesFieldName);
        const auto widthFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, widthFieldName);
        const auto depthFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, depthFieldName);
        const auto countMinDataFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, countMinDataFieldName);

        // Checking if all field offsets are valid and have a value
        if (!startTsFieldOffset.has_value() || !endTsFieldOffset.has_value() || !hashFieldOffset.has_value()
            || !observedTuplesFieldOffset.has_value() || !widthFieldOffset.has_value() || !depthFieldOffset.has_value()
            || !countMinDataFieldOffset.has_value()) {
            NES_ERROR("Expected to receive an offset for all required fields! Skipping creating a statistic for "
                      "the {}th tuple in the buffer!",
                      curTupleCnt);
            continue;
        }

        // We choose to hardcode here the values. If we would to do it dynamically during the runtime, we would have to
        // do a lot of branches, as we do not have here the tracing from Nautilus.
        const auto startTs = *reinterpret_cast<uint64_t*>(buffer.getBuffer() + startTsFieldOffset.value());
        const auto endTs = *reinterpret_cast<uint64_t*>(buffer.getBuffer() + endTsFieldOffset.value());
        const auto hash = *reinterpret_cast<uint64_t*>(buffer.getBuffer() + hashFieldOffset.value());
        const auto observedTuples = *reinterpret_cast<uint64_t*>(buffer.getBuffer() + observedTuplesFieldOffset.value());
        const auto width = *reinterpret_cast<uint64_t*>(buffer.getBuffer() + widthFieldOffset.value());
        const auto depth = *reinterpret_cast<uint64_t*>(buffer.getBuffer() + depthFieldOffset.value());

        // Reading the CountMinData that is stored as a string
        const auto countMinDataChildIdx = *reinterpret_cast<uint32_t*>(buffer.getBuffer() + countMinDataFieldOffset.value());
        const auto countMinDataString = Runtime::MemoryLayouts::readVarSizedData(buffer, countMinDataChildIdx);

        auto countMinStatistic = CountMinStatistic::create(Windowing::TimeMeasure(startTs),
                                                           Windowing::TimeMeasure(endTs),
                                                           observedTuples,
                                                           width,
                                                           depth,
                                                           countMinDataString);

        createdStatisticsWithTheirHash.emplace_back(hash, countMinStatistic);
    }

    return createdStatisticsWithTheirHash;
}
std::string CountMinStatisticSinkFormat::toString() const {
    std::ostringstream oss;
    oss << "startTsFieldName: " << startTsFieldName << " ";
    oss << "endTsFieldName: " << endTsFieldName << " ";
    oss << "statisticHashFieldName: " << statisticHashFieldName << " ";
    oss << "statisticTypeFieldName: " << statisticTypeFieldName << " ";
    oss << "observedTuplesFieldName: " << observedTuplesFieldName << " ";
    oss << "widthFieldName: " << widthFieldName << " ";
    oss << "depthFieldName: " << depthFieldName << " ";
    oss << "countMinDataFieldName: " << countMinDataFieldName << " ";
    return oss.str();
}

CountMinStatisticSinkFormat::~CountMinStatisticSinkFormat() = default;

}// namespace NES::Statistic
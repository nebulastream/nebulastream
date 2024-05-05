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
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Formats/StatisticCollection/DDSketchStatisticFormat.hpp>
#include <Statistics/Synopses/DDSketchStatistic.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>

namespace NES::Statistic {

StatisticFormatPtr DDSketchStatisticFormat::create(Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayoutInput,
                                                   std::function<std::string(const std::string&)> postProcessingData,
                                                   std::function<std::string(const std::string&)> preProcessingData) {
    const auto qualifierNameWithSeparator = memoryLayoutInput->getSchema()->getQualifierNameForSystemGeneratedFieldsWithSeparator();
    return std::make_shared<DDSketchStatisticFormat>(
        DDSketchStatisticFormat(qualifierNameWithSeparator, memoryLayoutInput, postProcessingData, preProcessingData));

}

std::vector<std::pair<StatisticHash, StatisticPtr>>
DDSketchStatisticFormat::readStatisticsFromBuffer(Runtime::TupleBuffer& buffer) {
    std::vector<HashStatisticPair> createdStatisticsWithTheirHash;
    
    // Each tuple represents one DDSketch
    for (auto curTupleCnt = 0_u64; curTupleCnt < buffer.getNumberOfTuples(); ++curTupleCnt) {
        // Calculating / Getting the field offset for each of the fields we need
        const auto startTsFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, startTsFieldName);
        const auto endTsFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, endTsFieldName);
        const auto hashFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, statisticHashFieldName);
        const auto observedTuplesFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, observedTuplesFieldName);
        const auto numberOfBucketsFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, numberOfBucketsFieldName);
        const auto gammaFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, gammaFieldName);
        const auto ddSketchDataFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, ddSketchDataFieldName);

        // Checking if all field offsets are valid and have a value
        if (!startTsFieldOffset.has_value() || !endTsFieldOffset.has_value() || !hashFieldOffset.has_value()
            || !observedTuplesFieldOffset.has_value() || !numberOfBucketsFieldOffset.has_value()
            || !gammaFieldOffset.has_value() || !ddSketchDataFieldOffset.has_value()) {
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
        const auto numberOfBuckets = *reinterpret_cast<uint64_t*>(buffer.getBuffer() + numberOfBucketsFieldOffset.value());
        const auto gamma = *reinterpret_cast<double*>(buffer.getBuffer() + gammaFieldOffset.value());

        // Reading the DDSketchData that is stored as a string
        const auto ddSketchDataChildIdx = *reinterpret_cast<uint32_t*>(buffer.getBuffer() + ddSketchDataFieldOffset.value());
        const auto ddSketchDataString =
            postProcessingData(Runtime::MemoryLayouts::readVarSizedData(buffer, ddSketchDataChildIdx));

        // Creating now a DDSketchStatistic from this
        auto ddSketchStatistic = DDSketchStatistic::create(Windowing::TimeMeasure(startTs),
                                                           Windowing::TimeMeasure(endTs),
                                                           observedTuples,
                                                           numberOfBuckets,
                                                           gamma,
                                                           ddSketchDataString);
        createdStatisticsWithTheirHash.emplace_back(hash, ddSketchStatistic);
    }

    return createdStatisticsWithTheirHash;
}

std::vector<Runtime::TupleBuffer>
DDSketchStatisticFormat::writeStatisticsIntoBuffers(const std::vector<HashStatisticPair>& statisticsPlusHashes,
                                                    Runtime::BufferManager& bufferManager) {
    std::vector<Runtime::TupleBuffer> createdTupleBuffers;
    uint64_t insertedStatistics = 0;
    const auto capacityOfTupleBuffer = memoryLayout->getCapacity();
    auto buffer = bufferManager.getBufferBlocking();
    for (auto& [statisticHash, statistic] : statisticsPlusHashes) {
        // 1. Calculating the offset for each field
        const auto startTsFieldOffset = memoryLayout->getFieldOffset(insertedStatistics, startTsFieldName);
        const auto endTsFieldOffset = memoryLayout->getFieldOffset(insertedStatistics, endTsFieldName);
        const auto hashFieldOffset = memoryLayout->getFieldOffset(insertedStatistics, statisticHashFieldName);
        const auto observedTuplesFieldOffset = memoryLayout->getFieldOffset(insertedStatistics, observedTuplesFieldName);
        const auto numberOfBucketsFieldOffset = memoryLayout->getFieldOffset(insertedStatistics, numberOfBucketsFieldName);
        const auto gammaFieldOffset = memoryLayout->getFieldOffset(insertedStatistics, gammaFieldName);
        const auto ddSketchFieldOffset = memoryLayout->getFieldOffset(insertedStatistics, ddSketchDataFieldName);

        // 2. Checking if all field offsets are valid and have a value
        if (!startTsFieldOffset.has_value() || !endTsFieldOffset.has_value() || !hashFieldOffset.has_value()
            || !observedTuplesFieldOffset.has_value() || !numberOfBucketsFieldOffset.has_value()
            || !gammaFieldOffset.has_value() || !ddSketchFieldOffset.has_value()) {
            NES_ERROR("Expected to receive an offset for all required fields! Skipping creating a statistic for "
                      "the {}th tuple in the buffer!",
                      insertedStatistics);
            continue;
        }

        // 3. Getting all values from the statistic
        NES_ASSERT2_FMT(statistic->instanceOf<DDSketchStatistic>(), "DDSketchStatisticSinkFormat knows only how to write DDSketchStatistics!");
        const auto ddSketchStatistic = statistic->as<DDSketchStatistic>();
        const auto startTs = ddSketchStatistic->getStartTs();
        const auto endTs = ddSketchStatistic->getEndTs();
        const auto hash = statisticHash;
        const auto observedTuples = ddSketchStatistic->getObservedTuples();
        const auto gamma = ddSketchStatistic->getGamma();
        const auto numberOfBuckets = ddSketchStatistic->getNumberOfBuckets();
        const auto data = preProcessingData(ddSketchStatistic->getDDSketchDataAsString());


        // 4. We choose to hardcode here the values. If we would to do it dynamically during the runtime, we would have to
        // do a lot of branches, as we do not have here the tracing from Nautilus.
        *reinterpret_cast<uint64_t*>(buffer.getBuffer() + startTsFieldOffset.value()) = startTs.getTime();
        *reinterpret_cast<uint64_t*>(buffer.getBuffer() + endTsFieldOffset.value()) = endTs.getTime();
        *reinterpret_cast<uint64_t*>(buffer.getBuffer() + hashFieldOffset.value()) = hash;
        *reinterpret_cast<uint64_t*>(buffer.getBuffer() + observedTuplesFieldOffset.value()) = observedTuples;
        *reinterpret_cast<double*>(buffer.getBuffer() + gammaFieldOffset.value()) = gamma;
        *reinterpret_cast<uint64_t*>(buffer.getBuffer() + numberOfBucketsFieldOffset.value()) = numberOfBuckets;

        // 5. Writing the DDSketchData that we store as a string
        const auto ddSketchDataChildIdx = Runtime::MemoryLayouts::writeVarSizedData(buffer, data, bufferManager);
        if (!ddSketchDataChildIdx.has_value()) {
            NES_ERROR("Expected to store the DD-Sketch data as a string! Skipping creating a statistic for "
                      "the {}th tuple in the buffer!",
                      insertedStatistics);
            continue;
        }
        *reinterpret_cast<uint32_t*>(buffer.getBuffer() + ddSketchFieldOffset.value()) = ddSketchDataChildIdx.value();

        // 6. Incrementing the position in the tuple buffer and setting the number of tuples
        insertedStatistics += 1;

        if (insertedStatistics >= capacityOfTupleBuffer) {
            buffer.setNumberOfTuples(insertedStatistics);
            createdTupleBuffers.emplace_back(buffer);

            buffer = bufferManager.getBufferBlocking();
            insertedStatistics = 0;
        }
    }

    // In the loop, we only insert into the createdTupleBuffers, if the buffer is full. Thus, we need to check, if we
    // have created a buffer that has not been inserted into createdTupleBuffers
    if (insertedStatistics > 0) {
        buffer.setNumberOfTuples(insertedStatistics);
        createdTupleBuffers.emplace_back(buffer);
    }

    return createdTupleBuffers;
}

std::string DDSketchStatisticFormat::toString() const {
    std::ostringstream oss;
    oss << "startTsFieldName: " << startTsFieldName << " ";
    oss << "endTsFieldName: " << endTsFieldName << " ";
    oss << "statisticHashFieldName: " << statisticHashFieldName << " ";
    oss << "statisticTypeFieldName: " << statisticTypeFieldName << " ";
    oss << "observedTuplesFieldName: " << observedTuplesFieldName << " ";
    oss << "numberOfBucketsFieldName: " << numberOfBucketsFieldName << " ";
    oss << "gammaFieldName: " << gammaFieldName << " ";
    return oss.str();
}

DDSketchStatisticFormat::DDSketchStatisticFormat(const std::string& qualifierNameWithSeparator,
                                                 Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayoutInput,
                                                 std::function<std::string(const std::string&)> postProcessingData,
                                                 std::function<std::string(const std::string&)> preProcessingData)
    : AbstractStatisticFormat(qualifierNameWithSeparator, memoryLayoutInput, postProcessingData, preProcessingData),
      numberOfBucketsFieldName(qualifierNameWithSeparator + WIDTH_FIELD_NAME),
      gammaFieldName(qualifierNameWithSeparator + GAMMA_FIELD_NAME),
      ddSketchDataFieldName(qualifierNameWithSeparator + STATISTIC_DATA_FIELD_NAME) {}

DDSketchStatisticFormat::~DDSketchStatisticFormat() = default;

}// namespace NES::Statistic

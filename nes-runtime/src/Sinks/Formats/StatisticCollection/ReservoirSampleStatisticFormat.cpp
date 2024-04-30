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
#include <Sinks/Formats/StatisticCollection/ReservoirSampleStatisticFormat.hpp>
#include <Statistics/Synopses/ReservoirSampleStatistic.hpp>
#include <Statistics/StatisticUtil.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>

namespace NES::Statistic {

StatisticFormatPtr
ReservoirSampleStatisticFormat::create(Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayoutInput,
                                       std::function<std::string(const std::string&)> postProcessingData,
                                       std::function<std::string(const std::string&)> preProcessingData) {
    const auto qualifierNameWithSeparator = memoryLayoutInput->getSchema()->getQualifierNameForSystemGeneratedFieldsWithSeparator();
    return std::make_shared<ReservoirSampleStatisticFormat>(
        ReservoirSampleStatisticFormat(qualifierNameWithSeparator, memoryLayoutInput, postProcessingData, preProcessingData));
}

std::vector<std::pair<StatisticHash, StatisticPtr>>
ReservoirSampleStatisticFormat::readStatisticsFromBuffer(Runtime::TupleBuffer& buffer) {
    std::vector<HashStatisticPair> createdStatisticsWithTheirHash;

    // Each tuple represents one Reservoir Sample
    for (auto curTupleCnt = 0_u64; curTupleCnt < buffer.getNumberOfTuples(); ++curTupleCnt) {
        // Calculating / Getting the field offset for each of the fields we need
        const auto startTsFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, startTsFieldName);
        const auto endTsFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, endTsFieldName);
        const auto hashFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, statisticHashFieldName);
        const auto observedTuplesFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, observedTuplesFieldName);
        const auto sampleSizeFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, sampleSizeFieldName);
        const auto sampleDataFieldOffset = memoryLayout->getFieldOffset(curTupleCnt, sampleDataFieldName);

        // Checking if all field offsets are valid and have a value
        if (!startTsFieldOffset.has_value() || !endTsFieldOffset.has_value() || !hashFieldOffset.has_value()
            || !observedTuplesFieldOffset.has_value() || !sampleSizeFieldOffset.has_value()
            || !sampleDataFieldOffset.has_value()) {
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
        const auto sampleSize = *reinterpret_cast<uint64_t*>(buffer.getBuffer() + sampleSizeFieldOffset.value());

        // Reading the ReservoirSampleData that is stored as a string
        const auto reservoirSampleDataChildIdx = *reinterpret_cast<uint32_t*>(buffer.getBuffer() + sampleDataFieldOffset.value());
        const auto reservoirSampleDataString = postProcessingData(Runtime::MemoryLayouts::readVarSizedData(buffer, reservoirSampleDataChildIdx));

        // Creating now a ReservoirSampleStatistic from this
        const auto sampleMemoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(sampleSchema, sampleSize * sampleSchema->getSchemaSizeInBytes());
        auto reservoirSampleStatistic = ReservoirSampleStatistic::create(Windowing::TimeMeasure(startTs),
                                                                         Windowing::TimeMeasure(endTs),
                                                                         observedTuples,
                                                                         sampleSize,
                                                                         reservoirSampleDataString,
                                                                         sampleMemoryLayout->getSchema());
        createdStatisticsWithTheirHash.emplace_back(hash, reservoirSampleStatistic);
    }

    return createdStatisticsWithTheirHash;
}

std::vector<Runtime::TupleBuffer>
ReservoirSampleStatisticFormat::writeStatisticsIntoBuffers(const std::vector<HashStatisticPair>& statisticsPlusHashes,
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
        const auto sampleSizeFieldOffset = memoryLayout->getFieldOffset(insertedStatistics, sampleSizeFieldName);
        const auto sampleDataFieldOffset = memoryLayout->getFieldOffset(insertedStatistics, sampleDataFieldName);

        // 2. Checking if all field offsets are valid and have a value
        if (!startTsFieldOffset.has_value() || !endTsFieldOffset.has_value() || !hashFieldOffset.has_value()
            || !observedTuplesFieldOffset.has_value() || !sampleSizeFieldOffset.has_value()
            || !sampleDataFieldOffset.has_value()) {
            NES_ERROR("Expected to receive an offset for all required fields! Skipping creating a statistic for "
                      "the {}th tuple in the buffer!",
                      insertedStatistics);
            continue;
        }

        // 3. Getting all values from the statistic
        NES_ASSERT2_FMT(statistic->instanceOf<ReservoirSampleStatistic>(),
                        "ReservoirSampleStatisticSinkFormat knows only how to write ReservoirSampleStatistics!");
        const auto reservoirSampleStatistic = statistic->as<ReservoirSampleStatistic>();
        const auto startTs = reservoirSampleStatistic->getStartTs();
        const auto endTs = reservoirSampleStatistic->getEndTs();
        const auto hash = statisticHash;
        const auto observedTuples = reservoirSampleStatistic->getObservedTuples();
        const auto sampleSize = reservoirSampleStatistic->getSampleSize();
        const auto data = preProcessingData(reservoirSampleStatistic->getReservoirSpaceAsString());

        // 4. We choose to hardcode here the values. If we would to do it dynamically during the runtime, we would have to
        // do a lot of branches, as we do not have here the tracing from Nautilus.
        *reinterpret_cast<uint64_t*>(buffer.getBuffer() + startTsFieldOffset.value()) = startTs.getTime();
        *reinterpret_cast<uint64_t*>(buffer.getBuffer() + endTsFieldOffset.value()) = endTs.getTime();
        *reinterpret_cast<uint64_t*>(buffer.getBuffer() + hashFieldOffset.value()) = hash;
        *reinterpret_cast<uint64_t*>(buffer.getBuffer() + observedTuplesFieldOffset.value()) = observedTuples;
        *reinterpret_cast<uint64_t*>(buffer.getBuffer() + sampleSizeFieldOffset.value()) = sampleSize;

        // 5. Writing the ReservoirSampleData that we store as a string
        const auto reservoirSampleDataChildIdx = Runtime::MemoryLayouts::writeVarSizedData(buffer, data, bufferManager);
        if (!reservoirSampleDataChildIdx.has_value()) {
            NES_ERROR("Expected to store the reservoir sample data as a string! Skipping creating a statistic for "
                      "the {}th tuple in the buffer!",
                      insertedStatistics);
            continue;
        }
        *reinterpret_cast<uint32_t*>(buffer.getBuffer() + sampleDataFieldOffset.value()) = reservoirSampleDataChildIdx.value();

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


std::string ReservoirSampleStatisticFormat::toString() const {
    std::ostringstream oss;
    oss << "startTsFieldName: " << startTsFieldName << " ";
    oss << "endTsFieldName: " << endTsFieldName << " ";
    oss << "statisticHashFieldName: " << statisticHashFieldName << " ";
    oss << "statisticTypeFieldName: " << statisticTypeFieldName << " ";
    oss << "observedTuplesFieldName: " << observedTuplesFieldName << " ";
    oss << "sampleSizeFieldName: " << sampleSizeFieldName << " ";
    oss << "sampleSchema: " << sampleSchema->toString() << " ";
    oss << "sampleDataFieldName: " << sampleDataFieldName << " ";
    return oss.str();
}

ReservoirSampleStatisticFormat::~ReservoirSampleStatisticFormat() = default;

ReservoirSampleStatisticFormat::ReservoirSampleStatisticFormat(const std::string& qualifierNameWithSeparator,
                                                               Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayoutInput,
                                                               std::function<std::string (const std::string&)> postProcessingData,
                                                               std::function<std::string (const std::string&)> preProcessingData)
    : AbstractStatisticFormat(qualifierNameWithSeparator, memoryLayoutInput, postProcessingData, preProcessingData),
      sampleSizeFieldName(qualifierNameWithSeparator + WIDTH_FIELD_NAME),
      sampleDataFieldName(qualifierNameWithSeparator + STATISTIC_DATA_FIELD_NAME) {

    // We have to call it here, as we have to make sure that the fields have been created
    sampleSchema = StatisticUtil::createSampleSchema(*memoryLayoutInput->getSchema());
}

}// namespace NES::Statistic
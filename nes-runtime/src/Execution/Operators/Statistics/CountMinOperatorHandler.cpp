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
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Statistics/CountMinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <StatisticFieldIdentifiers.hpp>
#include <Util/Core.hpp>

namespace NES::Experimental::Statistics {

CountMinOperatorHandler::CountMinOperatorHandler(uint64_t windowSize,
                                                 uint64_t slideFactor,
                                                 const std::string& logicalSourceName,
                                                 WorkerId workerId,
                                                 const std::string& fieldName,
                                                 uint64_t depth,
                                                 uint64_t width,
                                                 SchemaPtr schema,
                                                 std::vector<uint64_t> h3Seeds,
                                                 std::vector<std::pair<OriginId, std::string>>& allOriginIdsPhysicalSourceNames)
    : sliceAssigner(windowSize, slideFactor), logicalSourceName(logicalSourceName), workerId(workerId), fieldName(fieldName),
      depth(depth), width(width), h3Seeds(std::move(h3Seeds)), schema(std::move(schema)) {

    for (auto & allOriginIdsPhysicalSourceName : allOriginIdsPhysicalSourceNames) {
        auto originId = allOriginIdsPhysicalSourceName.first;
        auto physicalSourceName = allOriginIdsPhysicalSourceName.second;
        allPhysicalSources[originId] = physicalSourceName;
    }

    for (auto vectorIndex = 0UL; vectorIndex < allOriginIdsPhysicalSourceNames.size(); ++vectorIndex) {
        auto originId = allOriginIdsPhysicalSourceNames.at(vectorIndex).first;
        std::vector<OriginId> singleOriginVector = {originId};
        allWatermarkProcessors[allPhysicalSources.at(vectorIndex)] =
            std::make_unique<Runtime::Execution::Operators::MultiOriginWatermarkProcessor>(singleOriginVector);
    }
}

void CountMinOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr, uint32_t) {
    NES_DEBUG("start CountMinOperatorHandler");
}

void CountMinOperatorHandler::stop(Runtime::QueryTerminationType terminationType,
                                   Runtime::Execution::PipelineExecutionContextPtr) {
    NES_DEBUG("shutdown CountMinOperatorHandler: {}", terminationType);
}

Runtime::TupleBuffer CountMinOperatorHandler::writeMetaData(NES::Runtime::TupleBuffer buffer,
                                                            const Interval& interval,
                                                            const std::string& physicalSourceName) {

    std::vector<uint64_t> cmData(depth * width, 0);
    std::string cmString(reinterpret_cast<char*>(cmData.data()), cmData.size() * sizeof(uint64_t));

    auto dynBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(std::move(buffer), schema);
    dynBuffer[0].writeVarSized(LOGICAL_SOURCE_NAME, logicalSourceName, bufferManager.get());
    dynBuffer[0].writeVarSized(PHYSICAL_SOURCE_NAME, physicalSourceName, bufferManager.get());
    dynBuffer[0][OBSERVED_TUPLES].write<uint64_t>(workerId);
    dynBuffer[0].writeVarSized(FIELD_NAME, fieldName, bufferManager.get());

    dynBuffer[0][OBSERVED_TUPLES].write(0UL);
    dynBuffer[0][DEPTH].write(depth);
    dynBuffer[0][WIDTH].write(width);
    dynBuffer[0].writeVarSized(DATA, cmString, bufferManager.get());

    dynBuffer[0][START_TIME].write(interval.getStartTime());
    dynBuffer[0][END_TIME].write(interval.getEndTime());

    return dynBuffer.getBuffer();
}

Runtime::TupleBuffer CountMinOperatorHandler::getTupleBuffer(const std::string& physicalSourceName, uint64_t ts) {
    auto startTime = sliceAssigner.getSliceStartTs(ts);
    auto endTime = sliceAssigner.getSliceEndTs(ts);
    auto interval = Interval(startTime, endTime);

    for (auto& countMinBuffer : allCountMin) {
        auto dynBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(countMinBuffer, schema);

        auto sketchStartTime = dynBuffer[0][START_TIME].read<uint64_t>();
        auto sketchEndTime = dynBuffer[0][END_TIME].read<uint64_t>();
        auto sketchPhysicalSourceName = Runtime::MemoryLayouts::readVarSizedData(
            dynBuffer.getBuffer(),
            dynBuffer[0][PHYSICAL_SOURCE_NAME].read<Runtime::TupleBuffer::NestedTupleBufferKey>());

        auto sketchInterval = Interval(sketchStartTime, sketchEndTime);

        if (sketchInterval == interval && physicalSourceName == sketchPhysicalSourceName) {
            return countMinBuffer;
        }
    }

    NES_INFO("Creating a new tuple buffer to store a count min sketch for ts {} physicalSourceName {}", ts, physicalSourceName);
    auto tupleBuffer = writeMetaData(bufferManager->getBufferBlocking(), interval, physicalSourceName);
    tupleBuffer.setNumberOfTuples(1);
    tupleBuffer.setWatermark(interval.getEndTime());
    NES_INFO("buf: {}", NES::Util::printTupleBufferAsCSV(tupleBuffer, schema));

    allCountMin.emplace_back(tupleBuffer);

    return tupleBuffer;
}

void* CountMinOperatorHandler::getH3Seeds() const { return reinterpret_cast<void*>(const_cast<uint64_t*>(std::data(h3Seeds))); }

std::vector<Runtime::TupleBuffer>
CountMinOperatorHandler::getFinishedCountMinSketches(uint64_t localWatermarkTs, uint64_t sequenceNumber, NES::OriginId originId) {

    auto physicalSourceName = allPhysicalSources[originId];
    auto& multiOriginWatermarkProcessor = allWatermarkProcessors[physicalSourceName];
    auto currentGlobalWatermarkTS = multiOriginWatermarkProcessor->updateWatermark(localWatermarkTs, sequenceNumber, originId);

    NES_INFO("LocalWatermarkTS: {}   currentGlobalWatermarkTS: {}", localWatermarkTs, currentGlobalWatermarkTS);

    auto removeIt =
        std::remove_if(allCountMin.begin(), allCountMin.end(), [currentGlobalWatermarkTS](const Runtime::TupleBuffer& buf) {
            auto endTime = buf.getWatermark();
            return endTime <= currentGlobalWatermarkTS;
        });

    std::vector<Runtime::TupleBuffer> allFinishedCMSKetches(removeIt, allCountMin.end());
    allCountMin.erase(removeIt, allCountMin.end());

    return allFinishedCMSKetches;
}

void CountMinOperatorHandler::setBufferManager(const Runtime::BufferManagerPtr& bufferManager) {
    CountMinOperatorHandler::bufferManager = bufferManager;
}

}// namespace NES::Experimental::Statistics
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

#include <Runtime/QueryManager.hpp>
#include <Runtime/QueryStatistics.hpp>
#include <Sources/QueryStatisticSource.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <algorithm>
#include <cstdint>

namespace NES {

std::optional<Runtime::TupleBuffer> QueryStatisticSource::receiveData() {
    auto buffer = allocateBuffer();
    auto tupleBuffer = buffer.getBuffer();
    const auto bufferCapacity = buffer.getCapacity();

    // Writing all query statistics into leftOverQueryPlanIds
    for (auto& queryPlanId : queryManager->getAllRunningQueryPlanIds()) {
        nextQueryPlanIds.push_back(queryPlanId);
    }

    // As long as we can write the statistics into the buffer, we do so
    while (nextQueryPlanIds.size() > 0) {
        auto queryStatistic = queryManager->getQueryStatistics(nextQueryPlanIds.front());
        writeStatisticIntoBuffer(tupleBuffer, queryStatistic->copyAndReset());
        nextQueryPlanIds.pop_front();
        if (tupleBuffer.getNumberOfTuples() >= bufferCapacity) {
            return buffer.getBuffer();
        }
    }

    return {};
}

void QueryStatisticSource::writeStatisticIntoBuffer(Runtime::TupleBuffer buffer, const Runtime::QueryStatistics& statistic) {
    const auto currentTuplePosition = buffer.getNumberOfTuples();
    const auto processedTasksFieldOffset = memoryLayout->getFieldOffset(currentTuplePosition, processedTasksFieldName);
    const auto processedTuplesFieldOffset = memoryLayout->getFieldOffset(currentTuplePosition, processedTuplesFieldName);
    const auto processedBuffersFieldOffset = memoryLayout->getFieldOffset(currentTuplePosition, processedBuffersFieldName);
    const auto processedWatermarksFieldOffset = memoryLayout->getFieldOffset(currentTuplePosition, processedWatermarksFieldName);
    const auto latencySumFieldOffset = memoryLayout->getFieldOffset(currentTuplePosition, latencySumFieldName);
    const auto queueSizeSumFieldOffset = memoryLayout->getFieldOffset(currentTuplePosition, queueSizeSumFieldName);
    const auto availableGlobalBufferSumFieldOffset = memoryLayout->getFieldOffset(currentTuplePosition, availableGlobalBufferSumFieldName);
    const auto availableFixedBufferSumFieldOffset = memoryLayout->getFieldOffset(currentTuplePosition, availableFixedBufferSumFieldName);
    const auto timestampFirstProcessedTaskFieldNameOffset = memoryLayout->getFieldOffset(currentTuplePosition, timestampFirstProcessedTaskFieldName);
    const auto timestampLastProcessedTaskFieldNameOffset = memoryLayout->getFieldOffset(currentTuplePosition, timestampLastProcessedTaskFieldName);

    if (!processedTasksFieldOffset.has_value() || !processedTuplesFieldOffset.has_value() || !processedBuffersFieldOffset.has_value() ||
        !processedWatermarksFieldOffset.has_value() || !latencySumFieldOffset.has_value() || !queueSizeSumFieldOffset.has_value() ||
        !availableGlobalBufferSumFieldOffset.has_value() || !availableFixedBufferSumFieldOffset.has_value() ||
        !timestampFirstProcessedTaskFieldNameOffset.has_value() || !timestampLastProcessedTaskFieldNameOffset.has_value()) {
        NES_ERROR("Could not find all fields in the memory layout! Skipping the insertion!");
        return;
    }

    *reinterpret_cast<uint64_t*>(buffer.getBuffer() + processedTasksFieldOffset.value()) = statistic.getProcessedTasks();
    *reinterpret_cast<uint64_t*>(buffer.getBuffer() + processedTuplesFieldOffset.value()) = statistic.getProcessedTuple();
    *reinterpret_cast<uint64_t*>(buffer.getBuffer() + processedBuffersFieldOffset.value()) = statistic.getProcessedBuffers();
    *reinterpret_cast<uint64_t*>(buffer.getBuffer() + processedWatermarksFieldOffset.value()) = statistic.getProcessedWatermarks();
    *reinterpret_cast<uint64_t*>(buffer.getBuffer() + latencySumFieldOffset.value()) = statistic.getLatencySum();
    *reinterpret_cast<uint64_t*>(buffer.getBuffer() + queueSizeSumFieldOffset.value()) = statistic.getQueueSizeSum();
    *reinterpret_cast<uint64_t*>(buffer.getBuffer() + availableGlobalBufferSumFieldOffset.value()) = statistic.getAvailableGlobalBufferSum();
    *reinterpret_cast<uint64_t*>(buffer.getBuffer() + availableFixedBufferSumFieldOffset.value()) = statistic.getAvailableFixedBufferSum();
    *reinterpret_cast<uint64_t*>(buffer.getBuffer() + timestampFirstProcessedTaskFieldNameOffset.value()) = statistic.getTimestampFirstProcessedTask();
    *reinterpret_cast<uint64_t*>(buffer.getBuffer() + timestampLastProcessedTaskFieldNameOffset.value()) = statistic.getTimestampLastProcessedTask();

    // Incrementing the numberOfTuples in the buffer
    buffer.setNumberOfTuples(buffer.getNumberOfTuples() + 1);
}

std::string QueryStatisticSource::toString() const {

}

SourceType QueryStatisticSource::getType() const { return SourceType::QUERY_STATISTIC_SOURCE; }

QueryStatisticSource::QueryStatisticSource(
    const SchemaPtr& schema,
    const Runtime::BufferManagerPtr& bufferManager,
    const Runtime::QueryManagerPtr& queryManager,
    const OperatorId& operatorId,
    const OriginId& originId,
    StatisticId statisticId,
    size_t numSourceLocalBuffers,
    GatheringMode gatheringMode,
    const std::string& physicalSourceName,
    const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& executableSuccessors,
    uint64_t sourceAffinity,
    uint64_t taskQueueId)
    : DataSource(schema,
                 bufferManager,
                 queryManager,
                 operatorId,
                 originId,
                 statisticId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 physicalSourceName,
                 executableSuccessors,
                 sourceAffinity,
                 taskQueueId) {

    // TODO Tim hier die qualifierName vom Schema holen (gibt eine Funktion) und dann vor den Feldern hängen
    processedTasksFieldName = schema->getQualifierNameForSystemGeneratedFieldsWithSeparator() + PROCESSED_TASKS_FIELD_NAME;
}
}// namespace NES

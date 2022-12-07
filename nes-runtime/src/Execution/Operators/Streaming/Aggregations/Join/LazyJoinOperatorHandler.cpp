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


#include <atomic>
#include <cstddef>
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinUtil.hpp>


namespace NES::Runtime::Execution {

LazyJoinOperatorHandler::LazyJoinOperatorHandler(SchemaPtr joinSchemaLeft,
                                                 SchemaPtr joinSchemaRight,
                                                 std::string joinFieldNameLeft,
                                                 std::string joinFieldNameRight,
                                                 size_t maxNoWorkerThreads,
                                                 uint64_t counterFinishedBuildingStart,
                                                 size_t totalSizeForDataStructures,
                                                 size_t windowSize,
                                                 size_t pageSize,
                                                 size_t numPartitions)
                                                    : joinSchemaLeft(joinSchemaLeft), joinSchemaRight(joinSchemaRight),
                                                    joinFieldNameLeft(joinFieldNameLeft), joinFieldNameRight(joinFieldNameRight),
                                                    maxNoWorkerThreads(maxNoWorkerThreads), counterFinishedBuildingStart(counterFinishedBuildingStart),
                                                    counterFinishedSinkStart(numPartitions),
                                                    totalSizeForDataStructures(totalSizeForDataStructures),
                                                    lastTupleTimeStamp(windowSize - 1), windowSize(windowSize),
                                                    pageSize(pageSize), numPartitions(numPartitions) {

    NES_ASSERT2_FMT(0 != numPartitions, "NumPartitions is 0: " << numPartitions);
    size_t minRequiredSize = numPartitions * PREALLOCATED_SIZE;
    NES_ASSERT2_FMT(minRequiredSize < totalSizeForDataStructures, "Invalid size " << minRequiredSize << " < " << totalSizeForDataStructures);

    createNewWindow();
}



void LazyJoinOperatorHandler::recyclePooledBuffer(Runtime::detail::MemorySegment*) {}

void LazyJoinOperatorHandler::recycleUnpooledBuffer(Runtime::detail::MemorySegment*) {}

const std::string& LazyJoinOperatorHandler::getJoinFieldNameLeft() const { return joinFieldNameLeft; }
const std::string& LazyJoinOperatorHandler::getJoinFieldNameRight() const { return joinFieldNameRight; }

SchemaPtr LazyJoinOperatorHandler::getJoinSchemaLeft() const { return joinSchemaLeft; }
SchemaPtr LazyJoinOperatorHandler::getJoinSchemaRight() const { return joinSchemaRight; }

void LazyJoinOperatorHandler::start(PipelineExecutionContextPtr,StateManagerPtr, uint32_t) {
    NES_DEBUG("start LazyJoinOperatorHandler");
}

void LazyJoinOperatorHandler::stop(QueryTerminationType, PipelineExecutionContextPtr) {
    NES_DEBUG("stop LazyJoinOperatorHandler");
    // TODO ask Philipp, if I should delete here all windows
}

void LazyJoinOperatorHandler::createNewWindow() {
    NES_DEBUG("LazyJoinOperatorHandler: create a new window for the lazyjoin")
    lazyJoinWindows.emplace_back(maxNoWorkerThreads, counterFinishedBuildingStart, counterFinishedSinkStart,
                                 totalSizeForDataStructures, joinSchemaLeft->getSchemaSizeInBytes(),
                                 joinSchemaRight->getSchemaSizeInBytes(), lastTupleTimeStamp, pageSize, numPartitions);
}

void LazyJoinOperatorHandler::deleteWindow(size_t timeStamp) {
    for (auto it = lazyJoinWindows.begin(); it != lazyJoinWindows.end(); ++it) {
        if (timeStamp <= it->getLastTupleTimeStamp()) {
            lazyJoinWindows.erase(it);
            break;
        }
    }

}

LazyJoinWindow& LazyJoinOperatorHandler::getWindow(size_t timeStamp) {
    for (auto& lazyJoinWindow : lazyJoinWindows) {
        if (timeStamp <= lazyJoinWindow.getLastTupleTimeStamp()) {
            return lazyJoinWindow;
        }
    }

    NES_THROW_RUNTIME_ERROR("Could not find lazyJoinWindow for timestamp: " << timeStamp);

}

uint64_t LazyJoinOperatorHandler::getLastTupleTimeStamp() const {
    return lastTupleTimeStamp;
}

LazyJoinWindow& LazyJoinOperatorHandler::getWindowToBeFilled() {
    return getWindow(lastTupleTimeStamp);
}

void LazyJoinOperatorHandler::incLastTupleTimeStamp(uint64_t increment) {
    lastTupleTimeStamp += increment;
}

size_t LazyJoinOperatorHandler::getWindowSize() const {
    return windowSize;
}
size_t LazyJoinOperatorHandler::getNumPartitions() const { return numPartitions; }

size_t LazyJoinOperatorHandler::getNumActiveWindows() {
    return lazyJoinWindows.size();
}

} // namespace NES::Runtime::Execution
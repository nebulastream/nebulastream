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

#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <atomic>
#include <cstddef>

namespace NES::Runtime::Execution::Operators {

StreamJoinOperatorHandler::StreamJoinOperatorHandler(SchemaPtr joinSchemaLeft,
                                                     SchemaPtr joinSchemaRight,
                                                     std::string joinFieldNameLeft,
                                                     std::string joinFieldNameRight,
                                                     size_t maxNoWorkerThreads,
                                                     uint64_t counterFinishedBuildingStart,
                                                     size_t totalSizeForDataStructures,
                                                     size_t windowSize,
                                                     size_t pageSize,
                                                     size_t numPartitions)
    : joinSchemaLeft(joinSchemaLeft), joinSchemaRight(joinSchemaRight), joinFieldNameLeft(joinFieldNameLeft),
      joinFieldNameRight(joinFieldNameRight), maxNoWorkerThreads(maxNoWorkerThreads),
      counterFinishedBuildingStart(counterFinishedBuildingStart), counterFinishedSinkStart(numPartitions),
      totalSizeForDataStructures(totalSizeForDataStructures), lastTupleTimeStampLeft(windowSize - 1),
      lastTupleTimeStampRight(windowSize - 1), windowSize(windowSize), pageSize(pageSize), numPartitions(numPartitions) {

    NES_ASSERT2_FMT(0 != numPartitions, "NumPartitions is 0: " << numPartitions);
    size_t minRequiredSize = numPartitions * PREALLOCATED_SIZE;
    NES_ASSERT2_FMT(minRequiredSize < totalSizeForDataStructures,
                    "Invalid size " << minRequiredSize << " < " << totalSizeForDataStructures);

    // It does not matter here if we put true or false as a parameter
    createNewWindow(/* isLeftSide*/ true);
}

void StreamJoinOperatorHandler::recyclePooledBuffer(Runtime::detail::MemorySegment*) {}

void StreamJoinOperatorHandler::recycleUnpooledBuffer(Runtime::detail::MemorySegment*) {}

const std::string& StreamJoinOperatorHandler::getJoinFieldNameLeft() const { return joinFieldNameLeft; }
const std::string& StreamJoinOperatorHandler::getJoinFieldNameRight() const { return joinFieldNameRight; }

SchemaPtr StreamJoinOperatorHandler::getJoinSchemaLeft() const { return joinSchemaLeft; }
SchemaPtr StreamJoinOperatorHandler::getJoinSchemaRight() const { return joinSchemaRight; }

void StreamJoinOperatorHandler::start(PipelineExecutionContextPtr, StateManagerPtr, uint32_t) {
    NES_DEBUG("start StreamJoinOperatorHandler");
}

void StreamJoinOperatorHandler::stop(QueryTerminationType, PipelineExecutionContextPtr) {
    NES_DEBUG("stop StreamJoinOperatorHandler");
    // TODO ask Philipp, if I should delete here all windows
}

void StreamJoinOperatorHandler::createNewWindow(bool isLeftSide) {

    auto lastTupleTimeStamp = getLastTupleTimeStamp(isLeftSide);
    if (checkWindowExists(lastTupleTimeStamp)) {
        return;
    }

    NES_DEBUG("StreamJoinOperatorHandler: create a new window for the lazyjoin");
    streamJoinWindows.emplace_back(maxNoWorkerThreads,
                                   counterFinishedBuildingStart,
                                   counterFinishedSinkStart,
                                   totalSizeForDataStructures,
                                   joinSchemaLeft->getSchemaSizeInBytes(),
                                   joinSchemaRight->getSchemaSizeInBytes(),
                                   lastTupleTimeStamp,
                                   pageSize,
                                   numPartitions);
}

void StreamJoinOperatorHandler::deleteWindow(uint64_t timeStamp) {
    for (auto it = streamJoinWindows.begin(); it != streamJoinWindows.end(); ++it) {
        if (timeStamp <= it->getLastTupleTimeStamp()) {
            streamJoinWindows.erase(it);
            break;
        }
    }
}

bool StreamJoinOperatorHandler::checkWindowExists(uint64_t timeStamp) {
    for (auto& streamJoinWindow : streamJoinWindows) {
        if (timeStamp <= streamJoinWindow.getLastTupleTimeStamp()) {
            return true;
        }
    }

    return false;
}

StreamJoinWindow& StreamJoinOperatorHandler::getWindow(uint64_t timeStamp) {
    for (auto& streamJoinWindow : streamJoinWindows) {
        if (timeStamp <= streamJoinWindow.getLastTupleTimeStamp()) {
            return streamJoinWindow;
        }
    }

    NES_THROW_RUNTIME_ERROR("Could not find streamJoinWindow for timestamp: " << timeStamp);
}

uint64_t StreamJoinOperatorHandler::getLastTupleTimeStamp(bool isLeftSide) const {
    if (isLeftSide) {
        return lastTupleTimeStampLeft;
    } else {
        return lastTupleTimeStampRight;
    }
}

StreamJoinWindow& StreamJoinOperatorHandler::getWindowToBeFilled(bool isLeftSide) {
    if (isLeftSide) {
        return getWindow(lastTupleTimeStampLeft);
    } else {
        return getWindow(lastTupleTimeStampRight);
    }
}

void StreamJoinOperatorHandler::incLastTupleTimeStamp(uint64_t increment, bool isLeftSide) {
    if (isLeftSide) {
        lastTupleTimeStampLeft += increment;
    } else {
        lastTupleTimeStampRight += increment;
    }
}

size_t StreamJoinOperatorHandler::getWindowSize() const { return windowSize; }
size_t StreamJoinOperatorHandler::getNumPartitions() const { return numPartitions; }

size_t StreamJoinOperatorHandler::getNumActiveWindows() { return streamJoinWindows.size(); }

}// namespace NES::Runtime::Execution::Operators
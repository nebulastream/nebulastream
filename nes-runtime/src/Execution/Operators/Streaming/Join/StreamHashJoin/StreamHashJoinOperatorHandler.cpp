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

#include <Execution/Operators/Streaming/Join/StreamHashJoin/StreamHashJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <atomic>
#include <cstddef>

namespace NES::Runtime::Execution::Operators {

StreamHashJoinOperatorHandler::StreamHashJoinOperatorHandler(SchemaPtr joinSchemaLeft,
                                                             SchemaPtr joinSchemaRight,
                                                             std::string joinFieldNameLeft,
                                                             std::string joinFieldNameRight,
                                                             uint64_t counterFinishedBuildingStart,
                                                             size_t windowSize,
                                                             size_t totalSizeForDataStructures,
                                                             size_t pageSize,
                                                             size_t numPartitions)
    : joinSchemaLeft(joinSchemaLeft), joinSchemaRight(joinSchemaRight), joinFieldNameLeft(joinFieldNameLeft),
      joinFieldNameRight(joinFieldNameRight), counterFinishedBuildingStart(counterFinishedBuildingStart),
      counterFinishedSinkStart(numPartitions), totalSizeForDataStructures(totalSizeForDataStructures),
      lastTupleTimeStampLeft(windowSize - 1), lastTupleTimeStampRight(windowSize - 1), windowSize(windowSize), pageSize(pageSize),
      numPartitions(numPartitions) {

    NES_ASSERT2_FMT(0 < numPartitions, "NumPartitions is 0: " << numPartitions);
    size_t minRequiredSize = numPartitions * PREALLOCATED_SIZE;
    NES_ASSERT2_FMT(minRequiredSize < totalSizeForDataStructures,
                    "Invalid size " << minRequiredSize << " < " << totalSizeForDataStructures);
}

void StreamHashJoinOperatorHandler::recyclePooledBuffer(Runtime::detail::MemorySegment*) {}

void StreamHashJoinOperatorHandler::recycleUnpooledBuffer(Runtime::detail::MemorySegment*) {}

const std::string& StreamHashJoinOperatorHandler::getJoinFieldNameLeft() const { return joinFieldNameLeft; }
const std::string& StreamHashJoinOperatorHandler::getJoinFieldNameRight() const { return joinFieldNameRight; }

SchemaPtr StreamHashJoinOperatorHandler::getJoinSchemaLeft() const { return joinSchemaLeft; }
SchemaPtr StreamHashJoinOperatorHandler::getJoinSchemaRight() const { return joinSchemaRight; }

void StreamHashJoinOperatorHandler::start(PipelineExecutionContextPtr, StateManagerPtr, uint32_t) {
    NES_DEBUG("start HashJoinOperatorHandler");
}

void StreamHashJoinOperatorHandler::stop(QueryTerminationType, PipelineExecutionContextPtr) {
    NES_DEBUG("stop HashJoinOperatorHandler");
}

void StreamHashJoinOperatorHandler::setup(uint64_t newNumberOfWorkerThreads) {
    if (alreadySetup) {
        NES_DEBUG("HashJoinOperatorHandler::setup was called already!");
        return;
    }
    alreadySetup = true;

    NES_DEBUG("HashJoinOperatorHandler::setup was called!");
    // It does not matter here if we put true or false as a parameter
    this->numberOfWorkerThreads = newNumberOfWorkerThreads;
    createNewWindow(/**isLeftSide**/ true);
}

void StreamHashJoinOperatorHandler::createNewWindow(bool isLeftSide) {

    auto lastTupleTimeStamp = getLastTupleTimeStamp(isLeftSide);
    if (checkWindowExists(lastTupleTimeStamp)) {
        return;
    }

    auto windowStart = hashJoinWindows.size() * windowSize;
    auto windowEnd = windowStart + windowSize - 1;
    NES_DEBUG("HashJoinOperatorHandler: create a new window for the stream hash join [" << windowStart << ", " << windowEnd
                                                                                        << "]");

    hashJoinWindows.emplace_back(numberOfWorkerThreads,
                                 counterFinishedBuildingStart,
                                 counterFinishedSinkStart,
                                 totalSizeForDataStructures,
                                 joinSchemaLeft->getSchemaSizeInBytes(),
                                 joinSchemaRight->getSchemaSizeInBytes(),
                                 windowStart,
                                 windowEnd,
                                 pageSize,
                                 numPartitions);
}

void StreamHashJoinOperatorHandler::deleteWindow(uint64_t timeStamp) {
    for (auto it = hashJoinWindows.begin(); it != hashJoinWindows.end(); ++it) {
        if (timeStamp <= it->getWindowEnd()) {
            hashJoinWindows.erase(it);
            break;
        }
    }
}

bool StreamHashJoinOperatorHandler::checkWindowExists(uint64_t timeStamp) {
    for (auto& hashJoinWindow : hashJoinWindows) {
        if (timeStamp <= hashJoinWindow.getWindowEnd()) {
            return true;
        }
    }

    return false;
}

StreamHashJoinWindow& StreamHashJoinOperatorHandler::getWindow(uint64_t timeStamp) {
    for (auto& hashJoinWindow : hashJoinWindows) {
        if (timeStamp <= hashJoinWindow.getWindowEnd()) {
            return hashJoinWindow;
        }
    }

    NES_THROW_RUNTIME_ERROR("Could not find hashJoinWindow for timestamp: " << timeStamp);
}

uint64_t StreamHashJoinOperatorHandler::getLastTupleTimeStamp(bool isLeftSide) const {
    if (isLeftSide) {
        return lastTupleTimeStampLeft;
    } else {
        return lastTupleTimeStampRight;
    }
}

StreamHashJoinWindow& StreamHashJoinOperatorHandler::getWindowToBeFilled(bool isLeftSide) {
    if (isLeftSide) {
        return getWindow(lastTupleTimeStampLeft);
    } else {
        return getWindow(lastTupleTimeStampRight);
    }
}

void StreamHashJoinOperatorHandler::incLastTupleTimeStamp(uint64_t increment, bool isLeftSide) {
    if (isLeftSide) {
        lastTupleTimeStampLeft += increment;
    } else {
        lastTupleTimeStampRight += increment;
    }
}

size_t StreamHashJoinOperatorHandler::getWindowSize() const { return windowSize; }

size_t StreamHashJoinOperatorHandler::getNumPartitions() const { return numPartitions; }

size_t StreamHashJoinOperatorHandler::getNumActiveWindows() { return hashJoinWindows.size(); }

StreamHashJoinOperatorHandlerPtr StreamHashJoinOperatorHandler::create(const SchemaPtr& joinSchemaLeft,
                                                                       const SchemaPtr& joinSchemaRight,
                                                                       const std::string& joinFieldNameLeft,
                                                                       const std::string& joinFieldNameRight,
                                                                       uint64_t counterFinishedBuildingStart,
                                                                       size_t windowSize,
                                                                       size_t totalSizeForDataStructures,
                                                                       size_t pageSize,
                                                                       size_t numPartitions) {

    return std::make_shared<StreamHashJoinOperatorHandler>(joinSchemaLeft,
                                                           joinSchemaRight,
                                                           joinFieldNameLeft,
                                                           joinFieldNameRight,
                                                           counterFinishedBuildingStart,
                                                           windowSize,
                                                           totalSizeForDataStructures,
                                                           pageSize,
                                                           numPartitions);
}

const std::vector<OperatorId>& StreamHashJoinOperatorHandler::getJoinOperatorsId() const { return joinOperatorsId; }

void StreamHashJoinOperatorHandler::addOperatorId(OperatorId id) { joinOperatorsId.emplace_back(id); }

}// namespace NES::Runtime::Execution::Operators
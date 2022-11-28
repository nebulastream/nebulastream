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
                                                 uint64_t counterFinishedSinkStart,
                                                 size_t totalSizeForDataStructures,
                                                 size_t windowSize)
                                                    : joinSchemaLeft(joinSchemaLeft), joinSchemaRight(joinSchemaRight),
                                                    joinFieldNameLeft(joinFieldNameLeft), joinFieldNameRight(joinFieldNameRight),
                                                    maxNoWorkerThreads(maxNoWorkerThreads), counterFinishedBuildingStart(counterFinishedBuildingStart),
                                                    counterFinishedSinkStart(counterFinishedSinkStart),
                                                    totalSizeForDataStructures(totalSizeForDataStructures),
                                                    windowSize(windowSize) {
    createNewLocalHashTables();
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
}
void LazyJoinOperatorHandler::createNewLocalHashTables() {
    lazyJoinWindows.emplace(maxNoWorkerThreads, counterFinishedBuildingStart, counterFinishedSinkStart,
                                           totalSizeForDataStructures, joinSchemaLeft->getSchemaSizeInBytes(),
                                           joinSchemaRight->getSchemaSizeInBytes());
}

void LazyJoinOperatorHandler::deleteCurrentWindow() {
    lazyJoinWindows.pop();
}

LazyJoinWindow& LazyJoinOperatorHandler::getCurrentWindow() {
    return lazyJoinWindows.front();
}
size_t LazyJoinOperatorHandler::getWindowSize() const { return windowSize; }

} // namespace NES::Runtime::Execution
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

#ifndef NES_LAZYJOINWINDOW_HPP
#define NES_LAZYJOINWINDOW_HPP

#include <vector>
#include <Execution/Operators/Streaming/Join/LocalHashTable.hpp>
#include <Execution/Operators/Streaming/Join/SharedJoinHashTable.hpp>

namespace NES::Runtime::Execution {


class StreamJoinWindow {

  public:
    explicit StreamJoinWindow(size_t maxNoWorkerThreads, uint64_t counterFinishedBuildingStart, uint64_t counterFinishedSinkStart,
                            size_t totalSizeForDataStructures, size_t sizeOfRecordLeft, size_t sizeOfRecordRight,
                            size_t lastTupleTimeStamp, size_t pageSize, size_t numPartitions);
    virtual ~StreamJoinWindow();

    Operators::LocalHashTable& getLocalHashTable(size_t index, bool leftSide);

    Operators::SharedJoinHashTable& getSharedJoinHashTable(bool isLeftSide);

    uint64_t fetchSubBuild(uint64_t sub);

    uint64_t fetchSubSink(uint64_t sub);

    size_t getLastTupleTimeStamp() const;

  private:
    std::vector<Operators::LocalHashTable> localHashTableLeftSide;
    std::vector<Operators::LocalHashTable> localHashTableRightSide;
    Operators::SharedJoinHashTable leftSideHashTable;
    Operators::SharedJoinHashTable rightSideHashTable;
    std::atomic<uint64_t> counterFinishedBuilding;
    std::atomic<uint64_t> counterFinishedSink;
    uint8_t* head;
    std::atomic<uint64_t> tail;
    uint64_t overrunAddress;
    size_t lastTupleTimeStamp;
};

} // namespace NES::Runtime::Execution
#endif//NES_LAZYJOINWINDOW_HPP

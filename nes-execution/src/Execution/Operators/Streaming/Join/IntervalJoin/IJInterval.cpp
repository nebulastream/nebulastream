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

#include <Execution/Operators/Streaming/Join/IntervalJoin/IJInterval.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <sstream>
namespace NES::Runtime::Execution {

IJInterval::IJInterval(int64_t intervalStart,
                       int64_t intervalEnd,
                       uint64_t numberOfWorker,
                       BufferManagerPtr& bufferManager,
                       const SchemaPtr& leftSchema,
                       uint64_t leftPageSize,
                       const SchemaPtr& rightSchema,
                       uint64_t rightPageSize)
    : id(nextId++), bufferManager(bufferManager), leftSchema(leftSchema), rightSchema(rightSchema) {
    IJInterval::intervalStart = intervalStart;
    IJInterval::intervalEnd = intervalEnd;
    IJInterval::numWorkerThreads = numberOfWorker;
    IJInterval::leftPageSize = leftPageSize;
    IJInterval::rightPageSize = rightPageSize;

    leftTuples = std::make_unique<Nautilus::Interface::PagedVectorVarSized>(bufferManager, leftSchema, leftPageSize);

    for (uint64_t i = 0; i < numberOfWorker; ++i) {
        rightTuples.emplace_back(
            std::make_unique<Nautilus::Interface::PagedVectorVarSized>(bufferManager, rightSchema, rightPageSize));
    }

    NES_TRACE("Created IJInterval {} for {} workerThreads, resulting in {} rightTuples.size()",
              IJInterval::toString(),
              numberOfWorker,
              rightTuples.size());
}

uint64_t IJInterval::getNumberOfTuplesRight() {
    uint64_t sum = 0;
    for (auto& pagedVec : rightTuples) {
        sum += pagedVec->getNumberOfEntries();
    }
    return sum;
}

std::string IJInterval::toString() {
    std::ostringstream basicOstringStream;
    basicOstringStream << "Id :" << this->getId() << "( IntervalStart: " << intervalStart << " IntervalEnd: " << intervalEnd
                       << " rightNumberOfTuples: " << getNumberOfTuplesRight() << ")";
    return basicOstringStream.str();
}

void* IJInterval::getPagedVectorRefLeft() { return leftTuples.get(); }

// Each thread picks different page
void* IJInterval::getPagedVectorRefRight(WorkerThreadId workerThreadId) {
    const auto pos = workerThreadId % rightTuples.size();
    return rightTuples[pos].get();
}
uint64_t IJInterval::getId() const { return id; }

uint64_t IJInterval::nextId = 0;
int64_t IJInterval::getIntervalStart() const { return intervalStart; }
int64_t IJInterval::getIntervalEnd() const { return intervalEnd; }
};// namespace NES::Runtime::Execution

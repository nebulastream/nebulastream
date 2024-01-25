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

#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/ThreadLocalSliceStore.hpp>

#include <Execution/Operators/Streaming/Aggregations/WindowProcessingException.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/Synchronized.h>

namespace NES::Runtime::Execution::Operators {
template<class SliceType>
void ThreadLocalSliceStore<SliceType>::removeSlicesUntilTs(uint64_t ts) {
    // drop all slices as long as the list is not empty and the first slice ends before or at the current ts.
    auto lock = synchronizedSlices.wlock();
    while (!lock->empty() && lock->front()->getEnd() <= ts) {
        lock->pop_front();
    }
}
template<class SliceType>
ThreadLocalSliceStore<SliceType>::SliceTypePtr& ThreadLocalSliceStore<SliceType>::getFirstSlice() {
    auto lock = synchronizedSlices.rlock();
    return lock.asNonConstUnsafe().front();
}
template<class SliceType>
ThreadLocalSliceStore<SliceType>::SliceTypePtr& ThreadLocalSliceStore<SliceType>::getLastSlice() {
    auto lock = synchronizedSlices.rlock();
    return lock.asNonConstUnsafe().back();
}
template<class SliceType>
std::list<std::shared_ptr<SliceType>> ThreadLocalSliceStore<SliceType>::extractSlicesUntilTs(uint64_t ts) {
    // drop all slices as long as the list is not empty and the first slice ends before or at the current ts.
    auto lock = synchronizedSlices.wlock();
    auto& slices = *lock;
    std::list<std::shared_ptr<SliceType>> resultSlices;
    while (!slices.empty() && slices.front()->getEnd() <= ts) {
        resultSlices.emplace_back(std::move(slices.front()));
        lock->pop_front();
    }
    return resultSlices;
}
template<class SliceType>
ThreadLocalSliceStore<SliceType>::SliceTypePtr& ThreadLocalSliceStore<SliceType>::findSliceByTs(uint64_t ts) {
    if (ts < lastWatermarkTs) {
#ifndef UNIKERNEL_LIB
        throw WindowProcessingException("The ts " + std::to_string(ts) + " can't be smaller then the lastWatermarkTs "
                                        + std::to_string(lastWatermarkTs));
#else
        NES_ERROR("Window Error");
#endif
    }
    // get a read lock
    auto readLock = synchronizedSlices.rlock();
    auto& slices = readLock.asNonConstUnsafe();
    // Find the correct slice.
    // Reverse iteration over all slices from the end to the start,
    // as it is expected that ts is in a more recent slice.
    // At the end of the iteration sliceIter can be in three states:
    // 1. sliceIter == slices.rend() -> their is no slice with a start <= ts.
    // In this case we have to pre-pend a new slice.
    // 2. sliceIter is at a slide which slice.startTs < ts and slice.endTs <= ts.
    // In this case we have to insert a new slice after the current sliceIter
    // 3. sliceIter is at a slide which slice.startTs <= ts and slice.endTs > ts.
    // In this case we found the correct slice.
    auto sliceIter = slices.rbegin();
    while (sliceIter != slices.rend() && (*sliceIter)->getStart() > ts) {
        sliceIter++;
    }
    // Handle the individual cases and append a slice if required.
    if (sliceIter == slices.rend()) {
        // We are in case 1. thus we have to prepend a new slice
        auto newSliceStart = windowAssigner.getSliceStartTs(ts);
        auto newSliceEnd = windowAssigner.getSliceEndTs(ts);
        auto newSlice = allocateNewSlice(newSliceStart, newSliceEnd);
        readLock.unlock();
        return synchronizedSlices.wlock()->emplace_front(std::move(newSlice));
    } else if ((*sliceIter)->getStart() < ts && (*sliceIter)->getEnd() <= ts) {
        // We are in case 2. thus we have to append a new slice after the current iterator
        auto newSliceStart = windowAssigner.getSliceStartTs(ts);
        auto newSliceEnd = windowAssigner.getSliceEndTs(ts);
        auto newSlice = allocateNewSlice(newSliceStart, newSliceEnd);
        readLock.unlock();
        auto slice = synchronizedSlices.wlock()->emplace(sliceIter.base(), std::move(newSlice));
        return *slice;
    } else if ((*sliceIter)->coversTs(ts)) {
        // We are in case 3. and found a slice which covers the current ts.
        // Thus, we return a reference to the slice.
        return *sliceIter;
    } else {
        NES_THROW_RUNTIME_ERROR("Error during slice lookup: We looked for ts: "
                                << ts << " current front: " << getLastSlice()->getStart() << " - " << getLastSlice()->getEnd());
    }
}
template class ThreadLocalSliceStore<KeyedSlice>;
template class ThreadLocalSliceStore<NonKeyedSlice>;
}// namespace NES::Runtime::Execution::Operators
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

#include <Windowing/Experimental/TimeBasedWindow/KeyedSlice.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedThreadLocalSliceStore.hpp>

namespace NES::Windowing::Experimental {
KeyedThreadLocalSliceStore::KeyedThreadLocalSliceStore(NES::Experimental::HashMapFactoryPtr hashMapFactory,
                                                       uint64_t windowSize,
                                                       uint64_t windowSlide,
                                                       uint64_t)
    : hashMapFactory(hashMapFactory), windowSize(windowSize), windowSlide(windowSlide){};

KeyedSlicePtr KeyedThreadLocalSliceStore::allocateNewSlice(uint64_t startTs, uint64_t endTs, uint64_t sliceIndex) {
    return std::make_unique<KeyedSlice>(hashMapFactory, startTs, endTs, sliceIndex);
}

void KeyedThreadLocalSliceStore::removeSlicesUntilTs(uint64_t ts) {
    // drop all slices as long as the list is not empty and the first slice ends before the current ts.
    while (!slices.empty() && slices.front()->getEnd() <= ts) {
        slices.pop_front();
    }
}

uint64_t KeyedThreadLocalSliceStore::getLastWatermark() { return lastWatermarkTs; }

void KeyedThreadLocalSliceStore::setLastWatermark(uint64_t watermarkTs) { lastWatermarkTs = watermarkTs; }

uint64_t KeyedThreadLocalSliceStore::getNumberOfSlices() { return slices.size(); }

/*
KeyedSlicePtr& KeyedThreadLocalSliceStore::insertSlice(uint64_t sliceEnd) {
    // slices are always pre-initialized. Thus, we can just call reset.
    auto startTs = this->getNextSliceStart(sliceEnd * sliceSize);
    auto endTs = (sliceEnd + 1) * sliceSize;
    auto slice = allocateNewSlice(startTs, endTs, sliceEnd);
    if (sliceEnd >= lastIndex) {
        if (currentSlice != nullptr) {
            slices[currentSlice->getIndex()] = std::move(currentSlice);
        }
        lastIndex = sliceEnd;
        currentSlice = std::move(slice);
        return currentSlice;
    } else {
        slices[sliceEnd] = std::move(slice);
        return slices[sliceEnd];
    }
}
 */

}// namespace NES::Windowing::Experimental
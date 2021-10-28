/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_WINDOWING_RUNTIME_WINDOWED_JOIN_SLICE_LIST_STORE_HPP_
#define NES_INCLUDE_WINDOWING_RUNTIME_WINDOWED_JOIN_SLICE_LIST_STORE_HPP_

#include <Util/Logger.hpp>
#include <Windowing/Runtime/SliceMetaData.hpp>
#include <mutex>
#include <vector>

namespace NES::Windowing {

// TODO this is highly inefficient by design
// TODO to make this code perf friendly we have to throw it away xD
template<class ValueType>
class WindowedJoinSliceListStore {
  public:
    /**
        * @brief Checks if the slice store is empty.
        * @return true if empty.
        */
    inline uint64_t empty() {
        std::lock_guard lock(internalMutex);
        return sliceMetaData.empty();
    }

    /**
     * @return most current slice'index.
     */
    inline uint64_t getCurrentSliceIndex() {
        std::lock_guard lock(internalMutex);
        return sliceMetaData.size() - 1;
    }

    /**
     * @brief Gets the slice meta data.
     * @return vector of slice meta data.
     */
    inline std::vector<SliceMetaData>& getSliceMetadata() {
        std::lock_guard lock(internalMutex);
        return sliceMetaData;
    }

    /**
     * @return get slice index by timestamp or the first slice index if timestamp is -1
     */
    uint64_t getSliceIndexByTs(int64_t timestamp) {
        // XXX: @Philipp: is this the correct interpretation of timestamp == -1?
        //                In order to not copy code and enable the same logging, I return inside the loop.
        auto const isDefault = timestamp == -1;
        if (timestamp < 0 and !isDefault) {
            NES_ERROR("getSliceIndexByTs for could not find a slice, because the timestamp is "
                      "neither -1 nor positive: "
                      << timestamp);
            NES_THROW_RUNTIME_ERROR("getSliceIndexByTs for could not find a slice, this should not happen.");
        }
        auto const ts = isDefault ? 0ULL : static_cast<uint64_t>(timestamp);

        std::lock_guard lock(internalMutex);
        for (uint64_t i = 0ULL; i < sliceMetaData.size(); ++i) {
            auto slice = sliceMetaData[i];
            NES_TRACE("slice begin=" << slice.getStartTs() << " slice end =" << slice.getEndTs());
            if (isDefault || (slice.getStartTs() <= ts && slice.getEndTs() > ts)) {
                NES_TRACE("return slice id=" << i);
                return i;
            }
        }
        NES_ERROR("getSliceIndexByTs for could not find a slice, this should not happen ts" << timestamp << " " << ts);
        NES_THROW_RUNTIME_ERROR("getSliceIndexByTs for could not find a slice, this should not happen " << timestamp << " "
                                                                                                        << ts);
    }

    /**
     * @return reference to internal mutex to create complex locking semantics
     */
    std::recursive_mutex& mutex() const { return internalMutex; }

    /**
     * @return the internal append list
     */
    inline auto& getAppendList() {
        std::lock_guard lock(internalMutex);
        return content;
    }

    /**
 * @brief Remove slices between index 0 and pos.
 * @param pos the position till we want to remove slices.
 */
    inline void removeSlicesUntil(uint64_t watermark) {
        auto itSlice = sliceMetaData.begin();
        auto itAggs = content.begin();
        for (; itSlice != sliceMetaData.end(); ++itSlice) {
            if (itSlice->getEndTs() > watermark) {
                break;
            }
            NES_DEBUG("WindowedJoinSliceListStore removeSlicesUntil: watermark="
                      << watermark << " from slice endts=" << itSlice->getEndTs()
                      << " sliceMetaData size=" << sliceMetaData.size() << " content size=" << content.size());

            itAggs++;
        }

        sliceMetaData.erase(sliceMetaData.begin(), itSlice);
        content.erase(content.begin(), itAggs);
        NES_DEBUG("WindowedJoinSliceListStore: removeSlicesUntil size after cleanup slice=" << sliceMetaData.size()
                                                                                            << " content=" << content.size());
    }

    /**
     * @brief Add a slice to the store
     * @param slice the slice to add
     */
    inline void appendSlice(SliceMetaData slice) {
        NES_DEBUG("appendSlice with start " << slice.getStartTs());
        std::lock_guard lock(internalMutex);
        sliceMetaData.emplace_back(slice);
        content.emplace_back(std::vector<ValueType>());
    }

    /**
     * @brief Delete all content
     */
    inline void clear() {
        std::lock_guard lock(internalMutex);
        sliceMetaData.clear();
        content.clear();
    }

    /**
     * @brief add a value for a slice
     * @param index of the slice
     * @param value to append
     */
    inline void append(int64_t index, ValueType&& value) {
        std::lock_guard lock(internalMutex);
        NES_VERIFY(content.size() > index, "invalid index");
        content[index].emplace_back(std::move(value));
    }

    /**
    * @brief add a value for a slice
    * @param index of the slice
    * @param value to append
    */
    inline void append(int64_t index, ValueType& value) {
        std::lock_guard lock(internalMutex);
        NES_VERIFY(content.size() > index, "invalid index for content size" << content.size() << " idx=" << index);
        content[index].emplace_back(value);
    }

    std::atomic<uint64_t> nextEdge{};

  private:
    mutable std::recursive_mutex internalMutex;
    std::vector<SliceMetaData> sliceMetaData;
    /// slice index 0
    /// content [0] = [ 0, 1, 3]
    /// slice index 1
    /// content [1] = []
    /// outer index matches the slice index
    /// inner vector contains the tuples that fall within one slice
    std::vector<std::vector<ValueType>> content;
};

}// namespace NES::Windowing

#endif// NES_INCLUDE_WINDOWING_RUNTIME_WINDOWED_JOIN_SLICE_LIST_STORE_HPP_

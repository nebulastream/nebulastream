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

#ifndef NES_INCLUDE_WINDOWING_RUNTIME_WINDOWEDJOINSLICELISTSTORE_HPP_
#define NES_INCLUDE_WINDOWING_RUNTIME_WINDOWEDJOINSLICELISTSTORE_HPP_

#include <Util/Logger.hpp>
#include <Windowing/Runtime/SliceMetaData.hpp>
#include <mutex>
#include <vector>

namespace NES {

namespace Windowing {
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
        return sliceMetaData.empty(); }


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

    uint64_t getSliceIndexByTs(int64_t timestamp) {
        std::lock_guard lock(internalMutex);
        for (uint64_t i = 0; i < sliceMetaData.size(); i++) {
            auto slice = sliceMetaData[i];
            if (slice.getStartTs() <= timestamp && slice.getEndTs() > timestamp) {
                return i;
            }
        }
        return -1;
    }

    std::recursive_mutex& mutex() const {
        return internalMutex;
    }

    inline auto& getAppendList() {
        std::lock_guard lock(internalMutex);
        return content;
    }

    inline void appendSlice(SliceMetaData slice) {
        std::lock_guard lock(internalMutex);
        sliceMetaData.emplace_back(slice);
        content.emplace_back(std::vector<ValueType>());
    }

    inline void clear() {
        std::lock_guard lock(internalMutex);
        sliceMetaData.clear();
        content.clear();
    }

    inline void append(int64_t index, ValueType&& value) {
        std::lock_guard lock(internalMutex);
        NES_VERIFY(content.size() > index, "invalid index");
        content[index].emplace_back(std::move(value));
    }

    inline void append(int64_t index, ValueType& value) {
        std::lock_guard lock(internalMutex);
        NES_VERIFY(content.size() > index, "invalid index");
        content[index].emplace_back(value);
    }

  public:
    std::atomic<uint64_t> nextEdge;

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

}// namespace Windowing

}// namespace NES

#endif//NES_INCLUDE_WINDOWING_RUNTIME_WINDOWEDJOINSLICELISTSTORE_HPP_

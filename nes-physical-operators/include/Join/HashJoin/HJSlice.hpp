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

#pragma once

#include <cstdint>
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <SliceStore/Slice.hpp>
#include <HashMapSlice.hpp>

namespace NES
{

/// As a hash join has left and right side, we need to handle the left and right side of the join with one slice
/// Thus, we use a HashMapSlice and set the number of input streams to 2 in its constructor
class HJSlice final : public HashMapSlice
{
public:
    HJSlice(SliceStart sliceStart, SliceEnd sliceEnd, CreateNewHashMapSliceArgs createNewHashMapSliceArgs, uint64_t numberOfHashMaps);
    [[nodiscard]] Nautilus::Interface::HashMap* getHashMapPtr(WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide) const;
    [[nodiscard]] Nautilus::Interface::HashMap* getHashMapPtrOrCreate(WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide);
    [[nodiscard]] uint64_t getNumberOfHashMapsForSide() const;
};

}

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

namespace NES::Runtime::Execution
{

/// This class represents a single slice for a join. It stores all tuples for the left and right stream.
class StreamJoinSlice
{
public:
    StreamJoinSlice(SliceStart sliceStart, SliceEnd sliceEnd);
    virtual ~StreamJoinSlice() = default;

    [[nodiscard]] SliceStart getSliceStart() const;
    [[nodiscard]] SliceEnd getSliceEnd() const;

    [[nodiscard]] virtual uint64_t getNumberOfTuplesLeft() = 0;
    [[nodiscard]] virtual uint64_t getNumberOfTuplesRight() = 0;

    bool operator==(const StreamJoinSlice& rhs) const;
    bool operator!=(const StreamJoinSlice& rhs) const;

protected:
    const SliceStart sliceStart;
    const SliceEnd sliceEnd;
};
}

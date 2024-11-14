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

#include <Execution/Operators/Streaming/Join/StreamJoinSlice.hpp>

namespace NES::Runtime::Execution
{

StreamJoinSlice::StreamJoinSlice(const SliceStart sliceStart, const SliceEnd sliceEnd) : sliceStart(sliceStart), sliceEnd(sliceEnd)
{
}

SliceStart StreamJoinSlice::getSliceStart() const
{
    return sliceStart;
}

SliceEnd StreamJoinSlice::getSliceEnd() const
{
    return sliceEnd;
}

bool StreamJoinSlice::operator==(const StreamJoinSlice& rhs) const
{
    return (sliceStart == rhs.sliceStart && sliceEnd == rhs.sliceEnd);
}

bool StreamJoinSlice::operator!=(const StreamJoinSlice& rhs) const
{
    return !(rhs == *this);
}
}

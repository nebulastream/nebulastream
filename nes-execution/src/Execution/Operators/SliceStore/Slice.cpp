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

#include <Execution/Operators/SliceStore/Slice.hpp>

namespace NES::Runtime::Execution
{

Slice::Slice(const SliceStart sliceStart, const SliceEnd sliceEnd) : sliceStart(sliceStart), sliceEnd(sliceEnd)
{
}

Slice::Slice(const Slice& other) = default;
Slice::Slice(Slice&& other) noexcept = default;
Slice& Slice::operator=(const Slice& other) = default;
Slice& Slice::operator=(Slice&& other) noexcept = default;

SliceStart Slice::getSliceStart() const
{
    return sliceStart;
}

SliceEnd Slice::getSliceEnd() const
{
    return sliceEnd;
}

bool Slice::operator==(const Slice& rhs) const
{
    return (sliceStart == rhs.sliceStart && sliceEnd == rhs.sliceEnd);
}

bool Slice::operator!=(const Slice& rhs) const
{
    return !(rhs == *this);
}
}

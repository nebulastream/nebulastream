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

#include <Execution/Operators/Streaming/Join/StreamSlice.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <sstream>

namespace NES::Runtime::Execution {

uint64_t StreamSlice::getSliceIdentifier() const { return getSliceEnd(); }

uint64_t StreamSlice::getSliceStart() const { return sliceStart; }

uint64_t StreamSlice::getSliceEnd() const { return sliceEnd; }

StreamSlice::StreamSlice(uint64_t windowStart, uint64_t windowEnd)
    : sliceStart(windowStart), sliceEnd(windowEnd) {}

bool StreamSlice::operator==(const StreamSlice& rhs) const {
    return (sliceStart == rhs.sliceStart && sliceEnd == rhs.sliceEnd);
}

bool StreamSlice::operator!=(const StreamSlice& rhs) const { return !(rhs == *this); }

std::string StreamSlice::toString() {
    std::ostringstream basicOstringstream;
    basicOstringstream << "(sliceStart: " << sliceStart << " sliceEnd: " << sliceEnd << ")";
    return basicOstringstream.str();
}
}// namespace NES::Runtime::Execution
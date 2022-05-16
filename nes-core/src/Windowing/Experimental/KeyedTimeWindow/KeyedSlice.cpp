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

#include <Util/Experimental/HashMap.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedSlice.hpp>
namespace NES::Windowing::Experimental {

KeyedSlice::KeyedSlice(std::shared_ptr<NES::Experimental::HashMapFactory> hashMapFactory, uint64_t start, uint64_t end)
    : start(start), end(end), state(hashMapFactory->create()) {}

KeyedSlice::KeyedSlice(std::shared_ptr<NES::Experimental::HashMapFactory> hashMapFactory) : KeyedSlice(hashMapFactory, 0, 0) {}

void KeyedSlice::reset(uint64_t start, uint64_t end) {
    this->start = start;
    this->end = end;
    this->state.clear();
}
std::ostream& operator<<(std::ostream& os, const KeyedSlice& slice) {
    os << "start: " << slice.start << " end: " << slice.end;
    return os;
}
}// namespace NES::Windowing::Experimental
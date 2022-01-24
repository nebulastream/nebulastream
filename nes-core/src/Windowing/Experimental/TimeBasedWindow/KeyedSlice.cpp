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


#include <Windowing/Experimental/HashMap.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedSlice.hpp>
namespace NES::Windowing::Experimental {

KeyedSlice::KeyedSlice(std::shared_ptr<NES::Experimental::HashMapFactory> hashMapFactory,
                       uint64_t start,
                       uint64_t end,
                       uint64_t index)
    : start(start), end(end), index(index), state(hashMapFactory->create()) {}
KeyedSlice::KeyedSlice(std::shared_ptr<NES::Experimental::HashMapFactory> hashMapFactory) : KeyedSlice(hashMapFactory, 0, 0, 0) {}

void KeyedSlice::reset(uint64_t start, uint64_t end, uint64_t index) {
    this->start = start;
    this->end = end;
    this->index = index;
    this->state.clear();
}
}// namespace NES::Windowing::Experimental
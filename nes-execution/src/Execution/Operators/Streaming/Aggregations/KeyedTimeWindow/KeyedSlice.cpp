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
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime::Execution::Operators {

KeyedSlice::KeyedSlice(std::unique_ptr<Nautilus::Interface::ChainedHashMap> hashMap, uint64_t start, uint64_t end)
    : start(start), end(end), state(std::move(hashMap)) {}

KeyedSlice::~KeyedSlice() { NES_DEBUG("~KeyedSlice {}-{}", start, end); }

}// namespace NES::Runtime::Execution::Operators
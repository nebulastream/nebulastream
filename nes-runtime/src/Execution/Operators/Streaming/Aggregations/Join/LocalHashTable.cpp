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

#include <Execution/Operators/Streaming/Aggregations/Join/LocalHashTable.hpp>

namespace NES::Runtime::Execution::Operators {

void LocalHashTable::FixedPagesLinkedList::append(const uint64_t hash, const Nautilus::Record& record) {
    if (!curPage->append(hash, record)) {
        if (++pos < pages.size()) {
            curPage = pages[pos];
        } else {
            size_t recordSize = record.getSizeOfRecord();
            pages.emplace_back(curPage = new FixedPage(this->tail, overrunAddress, recordSize));
        }

    }
}


} // namespace NES::Runtime::Execution::Operators
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

#include <Windowing/Runtime/SliceMetaData.hpp>

namespace NES::Windowing {

SliceMetaData::SliceMetaData(uint64_t startTs, uint64_t endTs) : startTs(startTs), endTs(endTs), recordsPerSlice(0){};

uint64_t SliceMetaData::getEndTs() const { return endTs; }

uint64_t SliceMetaData::getStartTs() const { return startTs; }

void incrementRecordsPerSlice();

uint64_t SliceMetaData::getRecordsPerSlice() const { return recordsPerSlice; }

void SliceMetaData::incrementRecordsPerSlice() { recordsPerSlice++; }

void SliceMetaData::incrementRecordsPerSliceByValue(uint64_t value) { recordsPerSlice += value; }

}// namespace NES::Windowing
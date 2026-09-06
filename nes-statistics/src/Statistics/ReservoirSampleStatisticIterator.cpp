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

#include <Statistics/ReservoirSampleStatisticIterator.hpp>

#include <cstdint>
#include <functional>
#include <utility>
#include <vector>

#include <Interface/Record.hpp>
#include <Operators/Statistic/StatisticBlobType.hpp>
#include <Operators/Windows/Aggregations/ReservoirSampleAggregationLogicalFunction.hpp>
#include <Statistics/ReservoirSampleBlob.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

ReservoirSampleStatisticIterator::ReservoirSampleStatisticIterator(std::vector<ReservoirSampleField> sampleFields)
    : StatisticIterator(StatisticBlobType{ReservoirSampleAggregationLogicalFunction::NAME}), sampleFields(std::move(sampleFields))
{
}

uint64_t ReservoirSampleStatisticIterator::getExpectedPayloadSizeInBytes() const
{
    return 0;
}

void ReservoirSampleStatisticIterator::forEachRecord(const nautilus::val<int8_t*>& payload, const std::function<void(Record&)>& emit) const
{
    const auto tupleCount = readReservoirSampleBlobTupleCount(payload);
    ReservoirSampleBlobRowReader rowReader(payload, sampleFields);
    for (nautilus::val<uint64_t> i = 0; i < tupleCount; i = i + 1)
    {
        Record sampleRecord = rowReader.readNextTuple();
        emit(sampleRecord);
    }
}

}

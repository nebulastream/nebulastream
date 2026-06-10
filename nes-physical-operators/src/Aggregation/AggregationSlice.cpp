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
#include <Aggregation/AggregationSlice.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <SliceStore/Slice.hpp>
#include <ErrorHandling.hpp>
#include <HashMapSlice.hpp>

namespace NES
{
AggregationSlice::AggregationSlice(
    AbstractBufferProvider* bufferProvider,
    const SliceStart sliceStart,
    const SliceEnd sliceEnd,
    const CreateNewHashMapSliceArgs& createNewHashMapSliceArgs,
    const uint64_t numberOfHashMaps)
    : HashMapSlice(*bufferProvider, sliceStart, sliceEnd, createNewHashMapSliceArgs, numberOfHashMaps, 1)
{
}

const TupleBuffer* AggregationSlice::getHashMapBufferRefForWorker(const WorkerThreadId workerThreadId) const
{
    const auto hashMapIndex = workerThreadId % getNumberOfHashMaps();
    INVARIANT(hashMapIndex < getNumberOfHashMaps(), "The worker thread id should be smaller than the number of hashmaps");
    return getHashMapBufferRef(VariableSizedAccess::Index{hashMapIndex});
}

}

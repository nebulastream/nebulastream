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

#include <SliceStore/IntervalSliceStoreRef.hpp>

#include <memory>
#include <utility>
#include <Join/IntervalJoin/IntervalJoinOperatorHandler.hpp>
#include <SliceStore/CachingSliceStoreRef.hpp>
#include <SliceStore/IntervalSliceStore.hpp>
#include <SliceStore/SliceStoreRef.hpp>

namespace NES
{

/// Emit the interval-join specialization here, where the store and handler types are complete.
template class CachingSliceStoreRef<IntervalSliceStore, IntervalJoinOperatorHandler>;

std::unique_ptr<SliceStoreRef> IntervalSliceStore::createSliceStoreRef(
    IntervalSliceStoreRef::DataStructureExtractor extractor, IntervalSliceStoreRef::CreateSlicesFunction creator)
{
    return std::make_unique<IntervalSliceStoreRef>(sliceCacheConfiguration, this, std::move(extractor), std::move(creator));
}

}

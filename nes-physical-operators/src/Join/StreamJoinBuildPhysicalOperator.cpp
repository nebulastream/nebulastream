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
#include <Join/StreamJoinBuildPhysicalOperator.hpp>

#include <memory>
#include <utility>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/SliceStoreRef.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowBuildPhysicalOperator.hpp>

namespace NES
{

template <class DataStructureType>
StreamJoinBuildPhysicalOperator<DataStructureType>::StreamJoinBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    const JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout,
    std::unique_ptr<SliceStoreRef<DataStructureType>> sliceStoreRef)
    : WindowBuildPhysicalOperator<DataStructureType>(operatorHandlerId, std::move(timeFunction), std::move(sliceStoreRef))
    , joinBuildSide(joinBuildSide)
    , tupleLayout(std::move(tupleLayout))
{
}

/// Explicit instantiations for the data-structure types handed out by the slice store refs.
template class StreamJoinBuildPhysicalOperator<HashMap*>;
template class StreamJoinBuildPhysicalOperator<TupleBuffer*>;
}

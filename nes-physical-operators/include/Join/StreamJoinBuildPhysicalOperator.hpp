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

#pragma once

#include <cstdint>
#include <memory>

#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/SliceStoreRef.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowBuildPhysicalOperator.hpp>

namespace NES
{
/// This class is the first phase of the stream join. The actual implementation is not part of this class
/// @tparam DataStructureType the per-slice data-structure pointer the join build writes into
template <class DataStructureType>
class StreamJoinBuildPhysicalOperator : public WindowBuildPhysicalOperator<DataStructureType>
{
public:
    StreamJoinBuildPhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        JoinBuildSideType joinBuildSide,
        std::unique_ptr<TimeFunction> timeFunction,
        std::shared_ptr<PagedVectorTupleLayout> tupleLayout,
        std::unique_ptr<SliceStoreRef<DataStructureType>> sliceStoreRef);
    ~StreamJoinBuildPhysicalOperator() override = default;

    StreamJoinBuildPhysicalOperator(const StreamJoinBuildPhysicalOperator& other) = default;

protected:
    const JoinBuildSideType joinBuildSide;
    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members): immutable layout shared with derived build/probe operators.
    const std::shared_ptr<PagedVectorTupleLayout> tupleLayout;
};

}

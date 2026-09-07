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
#include <utility>
#include <Align/AlignDirectedOperatorHandler.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

template <typename EmitFn>
void tryResolvePendingDriving(
    ExecutionContext& ctx,
    OperatorHandlerId operatorHandlerId,
    TimeFunction& drivingTimeFunction,
    TimeFunction& searchedTimeFunction,
    const std::shared_ptr<PagedVectorTupleLayout>& pendingDrivingLayout,
    const std::shared_ptr<PagedVectorTupleLayout>& searchedLogLayout,
    EmitFn&& emit)
{
    const auto handler = ctx.getGlobalOperatorHandler(operatorHandlerId);

    const auto pendingDrivingBufPtr = nautilus::invoke(
        +[](OperatorHandler* handler) -> const TupleBuffer*
        { return dynamic_cast<AlignDirectedOperatorHandler&>(*handler).getPendingDrivingBuffer(); },
        handler);
    const auto searchedLogBufPtr = nautilus::invoke(
        +[](OperatorHandler* handler) -> const TupleBuffer*
        { return dynamic_cast<AlignDirectedOperatorHandler&>(*handler).getSearchedLogBuffer(); },
        handler);
    PagedVectorRef pendingDrivingRef{BorrowedNautilusBuffer::from(pendingDrivingBufPtr), pendingDrivingLayout};
    PagedVectorRef searchedLogRef{BorrowedNautilusBuffer::from(searchedLogBufPtr), searchedLogLayout};

    auto pendingDrivingFrontIndex = nautilus::invoke(
        +[](OperatorHandler* handler) { return dynamic_cast<AlignDirectedOperatorHandler&>(*handler).getPendingDrivingFrontIndex(); },
        handler);
    auto searchedLogFrontIndex = nautilus::invoke(
        +[](OperatorHandler* handler) { return dynamic_cast<AlignDirectedOperatorHandler&>(*handler).getSearchedLogFrontIndex(); },
        handler);
    const auto searchedLogSize = searchedLogRef.getNumberOfRecords();

    auto drivingIdx = nautilus::val<uint64_t>(0);
    for (auto drivingIt = pendingDrivingRef.begin(); drivingIt != pendingDrivingRef.end();
         ++drivingIt, drivingIdx = drivingIdx + nautilus::val<uint64_t>(1))
    {
        if (drivingIdx == pendingDrivingFrontIndex && searchedLogFrontIndex < searchedLogSize)
        {
            auto oldestDriving = *drivingIt;
            const auto oldestDrivingTs = drivingTimeFunction.getTs(ctx, oldestDriving);

            auto bestIdx = searchedLogFrontIndex;
            auto hasBefore = nautilus::val<bool>(false);
            auto foundAfter = nautilus::val<bool>(false);

            auto searchedIdx = nautilus::val<uint64_t>(0);
            for (auto searchedIt = searchedLogRef.begin(); searchedIt != searchedLogRef.end();
                 ++searchedIt, searchedIdx = searchedIdx + nautilus::val<uint64_t>(1))
            {
                if (searchedIdx >= searchedLogFrontIndex)
                {
                    auto candidate = *searchedIt;
                    const auto candidateTs = searchedTimeFunction.getTs(ctx, candidate);
                    if (candidateTs <= oldestDrivingTs)
                    {
                        bestIdx = searchedIdx;
                        hasBefore = nautilus::val<bool>(true);
                    }
                    else
                    {
                        foundAfter = nautilus::val<bool>(true);
                    }
                }
            }

            if (foundAfter)
            {
                if (hasBefore)
                {
                    auto bestCandidate = searchedLogRef.at(bestIdx);
                    Record output;
                    output.reassignFields(oldestDriving);
                    output.reassignFields(bestCandidate);
                    emit(ctx, output);
                    searchedLogFrontIndex = bestIdx;
                }
                pendingDrivingFrontIndex = pendingDrivingFrontIndex + nautilus::val<uint64_t>(1);
            }
        }
    }

    nautilus::invoke(
        +[](OperatorHandler* handler, uint64_t newFrontIndex)
        { dynamic_cast<AlignDirectedOperatorHandler&>(*handler).setPendingDrivingFrontIndex(newFrontIndex); },
        handler,
        pendingDrivingFrontIndex);
    nautilus::invoke(
        +[](OperatorHandler* handler, uint64_t newFrontIndex)
        { dynamic_cast<AlignDirectedOperatorHandler&>(*handler).setSearchedLogFrontIndex(newFrontIndex); },
        handler,
        searchedLogFrontIndex);
}

}

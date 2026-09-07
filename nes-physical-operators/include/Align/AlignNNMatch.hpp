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
#include <Align/AlignNNOperatorHandler.hpp>
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

namespace AlignNNMatchDetail
{
inline nautilus::val<uint64_t> distance(const nautilus::val<Timestamp>& a, const nautilus::val<Timestamp>& b)
{
    if (a > b)
    {
        return a.convertToValue() - b.convertToValue();
    }
    return b.convertToValue() - a.convertToValue();
}
}

template <typename EmitFn>
void tryResolvePendingLeft(
    ExecutionContext& ctx,
    OperatorHandlerId operatorHandlerId,
    TimeFunction& leftTimeFunction,
    TimeFunction& rightTimeFunction,
    const std::shared_ptr<PagedVectorTupleLayout>& pendingLeftLayout,
    const std::shared_ptr<PagedVectorTupleLayout>& rightLogLayout,
    EmitFn&& emit)
{
    const auto handler = ctx.getGlobalOperatorHandler(operatorHandlerId);

    const auto pendingLeftBufPtr = nautilus::invoke(
        +[](OperatorHandler* handler) -> const TupleBuffer*
        { return dynamic_cast<AlignNNOperatorHandler&>(*handler).getPendingLeftBuffer(); },
        handler);
    const auto rightLogBufPtr = nautilus::invoke(
        +[](OperatorHandler* handler) -> const TupleBuffer*
        { return dynamic_cast<AlignNNOperatorHandler&>(*handler).getRightLogBuffer(); },
        handler);
    PagedVectorRef pendingLeftRef{BorrowedNautilusBuffer::from(pendingLeftBufPtr), pendingLeftLayout};
    PagedVectorRef rightLogRef{BorrowedNautilusBuffer::from(rightLogBufPtr), rightLogLayout};

    auto pendingLeftFrontIndex = nautilus::invoke(
        +[](OperatorHandler* handler) { return dynamic_cast<AlignNNOperatorHandler&>(*handler).getPendingLeftFrontIndex(); }, handler);
    auto rightLogFrontIndex = nautilus::invoke(
        +[](OperatorHandler* handler) { return dynamic_cast<AlignNNOperatorHandler&>(*handler).getRightLogFrontIndex(); }, handler);
    const auto rightLogSize = rightLogRef.getNumberOfRecords();
    const auto pendingLeftSize = pendingLeftRef.getNumberOfRecords();

    auto leftIdx = nautilus::val<uint64_t>(0);
    for (auto leftIt = pendingLeftRef.begin(); leftIt != pendingLeftRef.end(); ++leftIt, leftIdx = leftIdx + nautilus::val<uint64_t>(1))
    {
        if (leftIdx == pendingLeftFrontIndex && rightLogFrontIndex < rightLogSize)
        {
            auto oldestLeft = *leftIt;
            const auto oldestLeftTs = leftTimeFunction.getTs(ctx, oldestLeft);

            auto bestIdx = rightLogFrontIndex;
            auto bestDistance = nautilus::val<uint64_t>(0);
            auto foundAfter = nautilus::val<bool>(false);

            auto rightIdx = nautilus::val<uint64_t>(0);
            for (auto rightIt = rightLogRef.begin(); rightIt != rightLogRef.end();
                 ++rightIt, rightIdx = rightIdx + nautilus::val<uint64_t>(1))
            {
                if (rightIdx >= rightLogFrontIndex)
                {
                    auto candidate = *rightIt;
                    const auto candidateTs = rightTimeFunction.getTs(ctx, candidate);
                    const auto candidateDistance = AlignNNMatchDetail::distance(oldestLeftTs, candidateTs);
                    if (rightIdx == rightLogFrontIndex || candidateDistance < bestDistance)
                    {
                        bestDistance = candidateDistance;
                        bestIdx = rightIdx;
                    }
                    foundAfter = foundAfter || (candidateTs > oldestLeftTs);
                }
            }

            if (foundAfter)
            {
                auto bestCandidate = rightLogRef.at(bestIdx);
                Record output;
                output.reassignFields(oldestLeft);
                output.reassignFields(bestCandidate);
                emit(ctx, output);

                rightLogFrontIndex = bestIdx;
                pendingLeftFrontIndex = pendingLeftFrontIndex + nautilus::val<uint64_t>(1);
            }
        }
    }

    nautilus::invoke(
        +[](OperatorHandler* handler, uint64_t newFrontIndex)
        { dynamic_cast<AlignNNOperatorHandler&>(*handler).setPendingLeftFrontIndex(newFrontIndex); },
        handler,
        pendingLeftFrontIndex);
    nautilus::invoke(
        +[](OperatorHandler* handler, uint64_t newFrontIndex)
        { dynamic_cast<AlignNNOperatorHandler&>(*handler).setRightLogFrontIndex(newFrontIndex); },
        handler,
        rightLogFrontIndex);
}

}

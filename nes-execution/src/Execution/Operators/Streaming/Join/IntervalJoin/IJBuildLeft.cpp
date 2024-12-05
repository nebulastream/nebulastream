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

#include <API/AttributeField.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/IntervalJoin/IJBuild.hpp>
#include <Execution/Operators/Streaming/Join/IntervalJoin/IJBuildLeft.hpp>
#include <Execution/Operators/Streaming/Join/IntervalJoin/IJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <iostream>

namespace NES::Runtime::Execution::Operators {

//// FUNCTION CALLS (ProxyFunctions)

/**
 * @brief creates and appends a new interval to the operatorHandler
 * return the index of current interval
 */
uint64_t createAndAppendIntervalProxy(void* operatorHandlerPtr, uint64_t timestampVal) {
    auto* ijOpHandler = static_cast<IJOperatorHandler*>(operatorHandlerPtr);

    ijOpHandler->createAndAppendNewInterval(timestampVal + ijOpHandler->getLowerBound(),
                                            timestampVal + ijOpHandler->getUpperBound());

    return (ijOpHandler->getNumberOfCurrentIntervals() - 1);
}

/**
 * @brief Proxy function for returning the pointer to the requested PagedVector from the operatorHandler
 */
void* getRightPagedVectorFromOperatorHandlerProxy(void* operatorHandlerPtr, WorkerThreadId workerThreadId) {
    NES_ASSERT2_FMT(operatorHandlerPtr != nullptr, "opHandler context should not be null!");
    auto* ijOperatorHandler = static_cast<IJOperatorHandler*>(operatorHandlerPtr);
    return ijOperatorHandler->getPagedVectorRefRight(workerThreadId);
}

/**
 * @brief finds the interval by index and returns either the left or right pagedvector
 */
void* getPagedVectorFromIntervalProxy(void* operatorHandlerPtr, uint64_t index) {
    NES_ASSERT2_FMT(operatorHandlerPtr != nullptr, "opHandler context should not be null!");
    auto* ijOperatorHandler = static_cast<IJOperatorHandler*>(operatorHandlerPtr);
    std::shared_ptr<IJInterval> interval = ijOperatorHandler->getInterval(index);
    return interval->getPagedVectorRefLeft();
}

void setIntervalStateLeftSideFilledProxy(void* operatorHandlerPtr, uint64_t index) {
    NES_ASSERT2_FMT(operatorHandlerPtr != nullptr, "opHandler context should not be null!");
    auto* ijOperatorHandler = static_cast<IJOperatorHandler*>(operatorHandlerPtr);
    std::shared_ptr<IJInterval> interval = ijOperatorHandler->getInterval(index);
    interval->intervalState = IJIntervalInfoState::LEFT_SIDE_FILLED;
}

int64_t getIntervalStartProxyIJBuild(void* intervalPtr) {
    NES_ASSERT2_FMT(intervalPtr != nullptr, "intervalPtr should not be null");
    auto* intervalPtrCast = static_cast<IJInterval*>(intervalPtr);
    return intervalPtrCast->getIntervalStart();
}

int64_t getIntervalEndProxyIJBuild(void* intervalPtr) {
    NES_ASSERT2_FMT(intervalPtr != nullptr, "intervalPtr should not be null");
    auto* intervalPtrCast = static_cast<IJInterval*>(intervalPtr);
    return intervalPtrCast->getIntervalEnd();
}

//// END FUNCTION CALLS (ProxyFunctions)

IJBuildLeft::IJBuildLeft(const uint64_t operatorHandlerIndex,
                         const SchemaPtr leftSchema,
                         const SchemaPtr rightSchema,
                         TimeFunctionPtr timeFunctionThisSide,
                         TimeFunctionPtr timeFunctionOtherSide)
    : IJBuild(operatorHandlerIndex, leftSchema, rightSchema, std::move(timeFunctionThisSide)),
      timeFunctionOtherSide(std::move(timeFunctionOtherSide)) {}

void IJBuildLeft::execute(ExecutionContext& ctx, Record& record) const {
    auto opHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto workerThreadId = ctx.getWorkerThreadId();
    // get timestamp of current tuple
    Value<UInt64> timestampVal = timeFunction->getTs(ctx, record);

    NES_DEBUG("Execute the leftSide record {} with timestamp {} and create a new interval and add all right tuple from opHandler "
              "to interval if fit "
              "into time range ",
              record.toString(),
              timestampVal.getValue().toString());
    // Create new Interval for each left tuple and add it to list of intervals maintained by the OpHandler
    auto indexOfNewInterval =
        Nautilus::FunctionCall("createAndAppendIntervalProxy", createAndAppendIntervalProxy, opHandlerMemRef, timestampVal);

    auto intervalLeftPagedVectorMemRef = Nautilus::FunctionCall("getPagedVectorFromIntervalProxy",
                                                                getPagedVectorFromIntervalProxy,
                                                                opHandlerMemRef,
                                                                indexOfNewInterval);

    Nautilus::Interface::PagedVectorVarSizedRef intervalPagedVectorLeftVarSizedRef(intervalLeftPagedVectorMemRef,
                                                                                   buildSideSchema);
    // Once created, the corresponding record is added to the leftPageVector and the interval's left side is marked as filled, i.e., an interval expects only a single tuple from the left side.
    intervalPagedVectorLeftVarSizedRef.writeRecord(record);

    Nautilus::FunctionCall("setIntervalStateLeftSideFilledProxy",
                           setIntervalStateLeftSideFilledProxy,
                           opHandlerMemRef,
                           indexOfNewInterval);

    auto intervalStart =
        FunctionCall("getIntervalStartProxyIJBuild", getIntervalStartProxyIJBuild, intervalLeftPagedVectorMemRef);
    auto intervalEnd = FunctionCall("getIntervalEndProxyIJBuild", getIntervalEndProxyIJBuild, intervalLeftPagedVectorMemRef);

    // get stored tuples from the right side of the workerThreadId
    auto rightPagedVector = Nautilus::FunctionCall("getRightPagedVectorFromOperatorHandlerProxy",
                                                   getRightPagedVectorFromOperatorHandlerProxy,
                                                   opHandlerMemRef,
                                                   workerThreadId);
    Nautilus::Interface::PagedVectorVarSizedRef rightTuplesPagedVectorVarSizedRef(rightPagedVector, otherSideSchema);

    // Iterate over all right tuples and if their timestamp is in the new created interval, add tuple to intervals right tuple page
    for (auto iter = rightTuplesPagedVectorVarSizedRef.begin(); iter != rightTuplesPagedVectorVarSizedRef.end(); ++iter) {
        Record currentRightRecord = *iter;

        auto timestampValCurrRightRecord = timeFunctionOtherSide->getTs(ctx, currentRightRecord);
        // TODO the code does not handle the excluding of bounds but it is possible in the Query #5059
        if (timestampValCurrRightRecord <= intervalEnd && timestampValCurrRightRecord >= intervalStart) {
            // 3. If tuple fits, write it to intervals right paged Vector
            auto intervalRightPagedVectorMemRef = Nautilus::FunctionCall("getPagedVectorFromIntervalProxy",
                                                                         getPagedVectorFromIntervalProxy,
                                                                         opHandlerMemRef,
                                                                         indexOfNewInterval);
            Nautilus::Interface::PagedVectorVarSizedRef intervalRightPagedVectorVarSizedRef(intervalRightPagedVectorMemRef,
                                                                                            otherSideSchema);
            intervalRightPagedVectorVarSizedRef.writeRecord(currentRightRecord);
        }
    }
}

}// namespace NES::Runtime::Execution::Operators

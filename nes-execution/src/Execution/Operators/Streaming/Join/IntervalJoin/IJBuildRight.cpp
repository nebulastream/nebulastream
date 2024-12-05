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
#include <Execution/Operators/Streaming/Join/IntervalJoin/IJBuildRight.hpp>
#include <Execution/Operators/Streaming/Join/IntervalJoin/IJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/IntervalJoin/IJProbe.hpp>
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
 * @brief finds the interval by index and returns either the left or right pagedvector
 */
void* getPagedVectorFromIntervalProxy(void* operatorHandlerPtr, WorkerThreadId workerThreadId, uint64_t index) {
    NES_ASSERT2_FMT(operatorHandlerPtr != nullptr, "opHandler context should not be null!");
    auto* ijOperatorHandler = static_cast<IJOperatorHandler*>(operatorHandlerPtr);
    std::shared_ptr<IJInterval> interval = ijOperatorHandler->getInterval(index);
    return interval->getPagedVectorRefRight(workerThreadId);
}

/**
 * @brief Proxy function for returning the pointer to the requested PagedVector from the operatorHandler
 */
void* getRightPagedVectorFromOperatorHandlerProxyIJBuildRight(void* operatorHandlerPtr, WorkerThreadId workerThreadId) {
    NES_ASSERT2_FMT(operatorHandlerPtr != nullptr, "opHandler context should not be null!");
    auto* ijOperatorHandler = static_cast<IJOperatorHandler*>(operatorHandlerPtr);
    return ijOperatorHandler->getPagedVectorRefRight(workerThreadId);
}

/**
 * Checks if the timestamp of the right tuple falls into the interval (inclusive borders only here)
 * // TODO the code does not handle the excluding of bounds but it is possible in the Query #5059
 */
bool checkTupleFitsIntervalProxyIJBuildRight(void* operatorHandlerPtr, uint64_t index, uint64_t rightRecordTimestampVal) {
    NES_ASSERT2_FMT(operatorHandlerPtr != nullptr, "opHandler context should not be null!");
    auto* ijOperatorHandler = static_cast<IJOperatorHandler*>(operatorHandlerPtr);
    std::shared_ptr<IJInterval> interval = ijOperatorHandler->getInterval(index);
    if (interval->getIntervalStart() <= (u_int) rightRecordTimestampVal
        && interval->getIntervalEnd() >= (u_int) rightRecordTimestampVal) {
        // tuple fits into interval
        return true;
    }
    return false;
}

/**
 * returns the number of intervals current intervals in the operatorHandler
 */
uint64_t getNumberOfIntervalsProxy(void* operatorHandlerPtr) {
    NES_ASSERT2_FMT(operatorHandlerPtr != nullptr, "opHandler context should not be null!");
    auto* ijOperatorHandler = static_cast<IJOperatorHandler*>(operatorHandlerPtr);
    NES_DEBUG("Number of current intervals is {}", ijOperatorHandler->getNumberOfCurrentIntervals());
    return ijOperatorHandler->getNumberOfCurrentIntervals();
}

//// END FUNCTION CALLS (ProxyFunctions)

IJBuildRight::IJBuildRight(const uint64_t operatorHandlerIndex,
                           const SchemaPtr buildSideSchema,
                           const SchemaPtr otherSideSchema,
                           TimeFunctionPtr timeFunction)
    : IJBuild(operatorHandlerIndex, buildSideSchema, otherSideSchema, std::move(timeFunction)) {}

void IJBuildRight::execute(ExecutionContext& ctx, Record& record) const {
    auto opHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto workerThreadId = ctx.getWorkerThreadId();
    // get timestamp of current right tuple
    Value<UInt64> timestampVal = timeFunction->getTs(ctx, record);
    NES_DEBUG("Printing record: {} with timestamp {}", record.toString(), timestampVal.getValue().toString());

    NES_DEBUG("Execute a rightSide Tuple: get all active Intervals and add tuple if it fits");
    // Get HandlerRightPageVectorStorage (= stores all right stream tuples)
    // TODO: we only have to store in case the lower bound is negative #5155
    auto handlerRightPagedVectorMemRef = Nautilus::FunctionCall("getRightPagedVectorFromOperatorHandlerProxy",
                                                                getRightPagedVectorFromOperatorHandlerProxyIJBuildRight,
                                                                opHandlerMemRef,
                                                                workerThreadId);
    // Write new right tuple to HandlerRightPageVectorStorage
    Nautilus::Interface::PagedVectorVarSizedRef handlerRightPagedVectorVarSizedRef(handlerRightPagedVectorMemRef,
                                                                                   buildSideSchema);
    handlerRightPagedVectorVarSizedRef.writeRecord(record);

    // Loop over all intervals and add current record to intervals if it fits the interval
    auto numberOfCurrentIntervals =
        Nautilus::FunctionCall("getNumberOfIntervalsProxy", getNumberOfIntervalsProxy, opHandlerMemRef);

    for (auto currentIntervalIndex = Value<UInt64>(0_u64); currentIntervalIndex < numberOfCurrentIntervals;
         currentIntervalIndex = currentIntervalIndex + 1) {
        // for each interval we have to get its start and end and check if the right tuple fits
        auto fitsInterval = Nautilus::FunctionCall("checkTupleFitsIntervalProxy",
                                                   checkTupleFitsIntervalProxyIJBuildRight,
                                                   opHandlerMemRef,
                                                   Value<Nautilus::UInt64>(currentIntervalIndex),
                                                   timestampVal);
        if (fitsInterval) {
            // get the fitting interval's right paged vector and add record to interval
            auto fittingIntervalRightPagedVector = Nautilus::FunctionCall("getPagedVectorFromIntervalProxy",
                                                                          getPagedVectorFromIntervalProxy,
                                                                          opHandlerMemRef,
                                                                          workerThreadId,
                                                                          currentIntervalIndex);

            Nautilus::Interface::PagedVectorVarSizedRef intervalPagedVectorRightVarSizedRef(fittingIntervalRightPagedVector,
                                                                                            buildSideSchema);
            intervalPagedVectorRightVarSizedRef.writeRecord(record);
        }
    }
}

}// namespace NES::Runtime::Execution::Operators

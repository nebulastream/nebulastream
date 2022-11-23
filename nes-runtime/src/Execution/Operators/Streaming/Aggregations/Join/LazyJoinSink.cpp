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

#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinSink.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <API/AttributeField.hpp>

namespace NES::Runtime::Execution::Operators {

void LazyJoinSink::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    auto partitionId = recordBuffer.getBuffer().load<UInt64>() - 1;
    LazyJoinOperatorHandlerPtr operatorHandler = static_cast<LazyJoinOperatorHandlerPtr>(ctx.getGlobalOperatorHandler(handlerIndex)->getValue());

    auto leftHashTable = operatorHandler->getSharedJoinHashTable(true /* isLeftSide */);
    auto rightHashTable = operatorHandler->getSharedJoinHashTable(false /* isLeftSide */);

    auto leftBucket = leftHashTable.getPagesForBucket(partitionId);
    auto rightBucket = rightHashTable.getPagesForBucket(partitionId);
    auto leftBucketSize = leftHashTable.getNumItems(partitionId);
    auto rightBucketSize = rightHashTable.getNumItems(partitionId);

    size_t joinedTuples = 0;
    if (leftBucketSize && rightBucketSize) {
        if (leftBucketSize > rightBucketSize) {
            joinedTuples = executeJoin(ctx, std::move(rightBucket), std::move(leftBucket));
        } else {
            joinedTuples = executeJoin(ctx, std::move(leftBucket), std::move(rightBucket));
        }
    }

    if (joinedTuples) {
        NES_DEBUG("Worker " << ctx.getWorkerId() << " got " << partitionId << " joined #tuple=" << joinedTuples);
        NES_ASSERT2_FMT(joinedTuples <= (leftBucketSize * rightBucketSize),
                        "Something wrong #joinedTuples= " << joinedTuples << " upper bound "
                                                          << (leftBucketSize * rightBucketSize));
    }
}
size_t LazyJoinSink::executeJoin(ExecutionContext& executionContext,
                                 std::vector<FixedPage>&& probeSide,
                                 std::vector<FixedPage>&& buildSide) const {
    size_t joinedTuples = 0;
    LazyJoinOperatorHandlerPtr operatorHandler = static_cast<LazyJoinOperatorHandlerPtr>(executionContext.getGlobalOperatorHandler(handlerIndex)->getValue());

    for(auto& lhsPage : probeSide) {
        auto lhsLen = lhsPage.size();
        for (auto i = 0UL; i < lhsLen; ++i) {
            auto& lhsRecord = lhsPage[i];
            auto lhsKey = lhsRecord.read(joinFieldName);
            for(auto& rhsPage : buildSide) {
                auto rhsLen = rhsPage.size();
                if (rhsLen == 0 || !rhsPage.bloomFilterCheck(lhsKey)) {
                    continue;
                }

                for (auto j = 0UL; j < rhsLen; ++j) {
                    auto& rhsRecord = rhsPage[j];
                    if (rhsRecord.read(joinFieldName) == lhsKey) {
                        // TODO ask Philipp if it makes more sense to store the records and then emit a full buffer
                        ++joinedTuples;

                        // TODO ask Philipp if I have to iterate over all fields
                        auto buffer = executionContext.allocateBuffer();
                        for (auto field : operatorHandler->getJoinSchema()->fields) {
                            buffer.store(rhsRecord.read(field->getName()));
                        }
                        executionContext.emitBuffer(RecordBuffer(buffer));
                    }
                }
            }

        }
    }

    return joinedTuples;
}
LazyJoinSink::LazyJoinSink(uint64_t handlerIndex, const std::string& joinFieldName)
    : handlerIndex(handlerIndex), joinFieldName(joinFieldName) {}

} //namespace NES::Runtime::Execution::Operators
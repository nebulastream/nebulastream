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

#include <atomic>

#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinBuild.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinUtil.hpp>

namespace NES::Runtime::Execution::Operators {

void LazyJoinBuild::execute(ExecutionContext& ctx, Record& record) const {
    // TODO check and see how we can differentiate, if the window is done and we can go to the merge part of the lazyjoin
    if (record.read("timestamp") < 0) {
        localHashTable.insert(record, joinFieldName);
    } else {
        for (auto a = 0; a < NUM_PARTITIONS; ++a) {
            sharedJoinHashTable.insertBucket(a, localHashTable.getBucketLinkedList(a));
        }

        if (counterFinishedBuilding.fetch_sub(1) == 1) {
            // If the last thread/worker is done with building, then start the probing phase

            // TODO ask Philipp how I can replicate this behavior
            auto queryManager = execCtx.getQueryManager();
            auto numOfWorkers = queryManager->getNumberOfWorkerThreads();
            for (auto i = 0; i < NUM_PARTITIONS; ++i) {
                auto partitionId = i + 1;
                auto buffer = ctx.allocateBuffer();
                buffer.store(ValuepartitionId);

                                  und dann emitBuffer() für addWorkForNextPipelineO

            }
        }


    }
}

} // namespace NES::Runtime::Execution::Operators
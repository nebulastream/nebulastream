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

#include <Catalogs/Query/DecomposedQueryPlanMetaData.hpp>
#include <chrono>

namespace NES {

DecomposedQueryPlanMetaDataPtr DecomposedQueryPlanMetaData::create(DecomposedQueryPlanId decomposedQueryPlanId,
                                                                   QueryState decomposedQueryPlanState,
                                                                   WorkerId workerId) {
    return std::make_shared<DecomposedQueryPlanMetaData>(decomposedQueryPlanId, decomposedQueryPlanState, workerId);
}

DecomposedQueryPlanMetaData::DecomposedQueryPlanMetaData(DecomposedQueryPlanId querySubPlanId,
                                                         QueryState decomposedQueryPlanState,
                                                         WorkerId workerId)
    : decomposedQueryPlanId(querySubPlanId), decomposedQueryPlanState(decomposedQueryPlanState), workerId(workerId) {}

void DecomposedQueryPlanMetaData::updateState(QueryState newDecomposedQueryPlanState) {
    decomposedQueryPlanState = newDecomposedQueryPlanState;
    uint64_t usSinceEpoch =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    history.emplace_back(usSinceEpoch, decomposedQueryPlanState);
}

void DecomposedQueryPlanMetaData::setTerminationReason(const std::string& terminationReason) {
    this->terminationReason = terminationReason;
}

DecomposedQueryPlanId DecomposedQueryPlanMetaData::getDecomposedQueryPlanId() const { return decomposedQueryPlanId; }

QueryState DecomposedQueryPlanMetaData::getDecomposedQueryPlanStatus() const { return decomposedQueryPlanState; }

uint64_t DecomposedQueryPlanMetaData::getWorkerId() const { return workerId; }

const std::string& DecomposedQueryPlanMetaData::getTerminationReason() const { return terminationReason; }

const QueryStateHistory& DecomposedQueryPlanMetaData::getHistory() const { return history; }

}// namespace NES
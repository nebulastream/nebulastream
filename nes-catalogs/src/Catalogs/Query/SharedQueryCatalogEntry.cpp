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
#include <Catalogs/Query/SharedQueryCatalogEntry.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Catalogs::Query {

SharedQueryCatalogEntry::SharedQueryCatalogEntry(SharedQueryId sharedQueryId, std::set<QueryId> queryIds, QueryState queryState)
    : sharedQueryId(sharedQueryId), containedQueryIds(queryIds), queryState(queryState) {}

QueryState SharedQueryCatalogEntry::getQueryState() const { return queryState; }

void SharedQueryCatalogEntry::setQueryState(QueryState queryStatus) {
    this->queryState = queryStatus;
    uint64_t usSinceEpoch =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    history.emplace_back(usSinceEpoch, queryStatus);
}

void SharedQueryCatalogEntry::setTerminationReason(std::string terminationReason) { this->terminationReason = terminationReason; }

std::set<QueryId> SharedQueryCatalogEntry::getContainedQueryIds() { return containedQueryIds; }

void SharedQueryCatalogEntry::addQueryId(NES::QueryId queryId) { containedQueryIds.emplace(queryId); }

std::vector<DecomposedQueryPlanMetaDataPtr> SharedQueryCatalogEntry::getAllDecomposedQueryPlanMetaData() {
    std::vector<DecomposedQueryPlanMetaDataPtr> decomposedQueryPlan;
    for (const auto& [decomposedQueryId, decomposedQueryPlanMetaDatum] : decomposedQueryPlanMetaData) {
        decomposedQueryPlan.emplace_back(decomposedQueryPlanMetaDatum);
    }
    return decomposedQueryPlan;
}

DecomposedQueryPlanMetaDataPtr
SharedQueryCatalogEntry::getDecomposedQueryPlanMetaData(DecomposedQueryPlanId decomposedQueryPlanId) {
    if (!decomposedQueryPlanMetaData.contains(decomposedQueryPlanId)) {
        NES_ERROR("No decomposed query plan with id {} exists.", decomposedQueryPlanId);
    }
    return decomposedQueryPlanMetaData[decomposedQueryPlanId];
}

void SharedQueryCatalogEntry::addDecomposedQueryPlanMetaData(DecomposedQueryPlanId decomposedQueryPlanId,
                                                             DecomposedQueryPlanVersion decomposedQueryPlanVersion,
                                                             WorkerId workerId,
                                                             QueryState decomposedQueryPlanState) {

    auto decomposedQueryPlanMetaDatum = DecomposedQueryPlanMetaData::create(decomposedQueryPlanId,
                                                                            decomposedQueryPlanVersion,
                                                                            decomposedQueryPlanState,
                                                                            workerId);
    decomposedQueryPlanMetaData[decomposedQueryPlanId] = std::move(decomposedQueryPlanMetaDatum);
}

SharedQueryId SharedQueryCatalogEntry::getSharedQueryId() const noexcept { return sharedQueryId; }

const QueryStateHistory& SharedQueryCatalogEntry::getHistory() const { return history; }

std::string SharedQueryCatalogEntry::getTerminationReason() { return terminationReason; }

}// namespace NES::Catalogs::Query
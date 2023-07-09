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

#include <Catalogs/Query/QuerySubPlanMetaData.hpp>

namespace NES {

QuerySubPlanMetaDataPtr
QuerySubPlanMetaData::create(QuerySubPlanId querySubPlanId, QueryState subQueryStatus, uint64_t workerId) {
    return std::make_shared<QuerySubPlanMetaData>(querySubPlanId, subQueryStatus, workerId);
}

QuerySubPlanMetaData::QuerySubPlanMetaData(QuerySubPlanId querySubPlanId, NES::QueryState subQueryStatus, uint64_t workerId)
    : querySubPlanId(querySubPlanId), subQueryStatus(subQueryStatus), workerId(workerId) {}

void QuerySubPlanMetaData::updateStatus(QueryState queryStatus) { subQueryStatus = queryStatus; }

void QuerySubPlanMetaData::updateMetaInformation(const std::string& metaInformation) { this->metaInformation = metaInformation; }

QueryState QuerySubPlanMetaData::getQuerySubPlanStatus() { return subQueryStatus; }

QuerySubPlanId QuerySubPlanMetaData::getQuerySubPlanId() const { return querySubPlanId; }

QueryState QuerySubPlanMetaData::getSubQueryStatus() const { return subQueryStatus; }

uint64_t QuerySubPlanMetaData::getWorkerId() const { return workerId; }

const std::string& QuerySubPlanMetaData::getMetaInformation() const { return metaInformation; }

}// namespace NES
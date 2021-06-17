/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Plans/Query/QueryReconfigurationPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Util/Logger.hpp>
#include <sstream>

namespace NES {

QueryReconfigurationPlan::QueryReconfigurationPlan(
    const std::vector<QuerySubPlanId> querySubPlanIdsToStart,
    const std::vector<QuerySubPlanId> querySubPlanIdsToStop,
    const std::unordered_map<QuerySubPlanId, QuerySubPlanId> querySubPlanIdsToReplace)
    : id(PlanIdGenerator::getNextQueryReconfigurationPlanId()), querySubPlanIdsToStop(querySubPlanIdsToStop),
      querySubPlanIdsToStart(querySubPlanIdsToStart), querySubPlanIdsToReplace(querySubPlanIdsToReplace) {}

const std::vector<QuerySubPlanId> QueryReconfigurationPlan::getQuerySubPlanIdsToStop() const { return querySubPlanIdsToStop; }
const std::vector<QuerySubPlanId> QueryReconfigurationPlan::getQuerySubPlanIdsToStart() const { return querySubPlanIdsToStart; }
const std::unordered_map<QuerySubPlanId, QuerySubPlanId> QueryReconfigurationPlan::getQuerySubPlanIdsToReplace() const {
    return querySubPlanIdsToReplace;
}

QueryReconfigurationPlanPtr
QueryReconfigurationPlan::create(const std::vector<QuerySubPlanId> querySubPlanIdsToStart,
                                 const std::vector<QuerySubPlanId> querySubPlanIdsToStop,
                                 const std::unordered_map<QuerySubPlanId, QuerySubPlanId> querySubPlanIdsToReplace) {
    return std::make_shared<QueryReconfigurationPlan>(querySubPlanIdsToStart, querySubPlanIdsToStop, querySubPlanIdsToReplace);
}

const std::string QueryReconfigurationPlan::serializeToString() {
    std::stringstream ss;
    ss << "reconfigurationId:" << id << "|";
    ss << "querySubPlanIdsToStart:";
    for (auto querySubPlanIdToStart : querySubPlanIdsToStart) {
        ss << querySubPlanIdToStart << " ";
    }
    ss << "|";
    ss << "querySubPlanIdsToStop:";
    for (auto querySubPlanIdToStop : querySubPlanIdsToStop) {
        ss << querySubPlanIdToStop << " ";
    }
    ss << "|";
    ss << "querySubPlanIdsToReplace:";
    for (auto querySubPlanIdToReplace : querySubPlanIdsToReplace) {
        ss << querySubPlanIdToReplace.first << " " << querySubPlanIdToReplace.second << " ";
    }
    ss << "|";
    return ss.str();
}

QueryReconfigurationId QueryReconfigurationPlan::getId() const { return id; }
void QueryReconfigurationPlan::setId(QueryReconfigurationId reconfigurationId) { id = reconfigurationId; }

std::vector<QuerySubPlanId> lineToValues(std::string& line) {
    std::string valueDelimiter = " ";
    std::vector<QuerySubPlanId> values;
    size_t pos;
    while ((pos = line.find(valueDelimiter)) != std::string::npos) {
        auto valueToken = line.substr(0, pos);
        if (!valueToken.empty()) {
            values.push_back(std::stoull(valueToken));
        }
        line.erase(0, pos + valueDelimiter.length());
    }
    return values;
}

void parseFields(QueryReconfigurationPlanPtr queryReconfigurationPlan, std::string line) {
    std::string fieldDelimiter = ":";
    size_t pos;
    if ((pos = line.find(fieldDelimiter)) != std::string::npos) {
        auto field = line.substr(0, pos);
        line.erase(0, pos + fieldDelimiter.length());
        if (field == "reconfigurationId") {
            queryReconfigurationPlan->setId(std::stoull(line));
        } else if (field == "querySubPlanIdsToStart") {
            queryReconfigurationPlan->setQuerySubPlanIdsToStart(lineToValues(line));
        } else if (field == "querySubPlanIdsToStop") {
            queryReconfigurationPlan->setQuerySubPlanIdsToStop(lineToValues(line));
        } else if (field == "querySubPlanIdsToReplace") {
            std::unordered_map<uint64_t, uint64_t> querySubPlanIdsToReplace;
            std::vector<uint64_t> pairs = lineToValues(line);

            NES_ASSERT2_FMT(pairs.size() % 2 == 0, "Invalid configuration for QuerySubPlan replacement mapping.");

            for (int i = 0; i < pairs.size(); i += 2) {
                querySubPlanIdsToReplace.insert(std::pair<uint64_t, uint64_t>(pairs[i], pairs[i + 1]));
            }
            queryReconfigurationPlan->setQuerySubPlanIdsToReplace(querySubPlanIdsToReplace);
        } else {
            NES_ERROR("Encountered unknown field: " << field);
        }
    }
}

QueryReconfigurationPlanPtr QueryReconfigurationPlan::deserializeFromString(std::string serializedString) {
    auto queryReconfigurationPlan = std::make_shared<QueryReconfigurationPlan>();
    std::string lineDelimiter = "|";
    size_t pos;
    while ((pos = serializedString.find(lineDelimiter)) != std::string::npos) {
        auto token = serializedString.substr(0, pos);
        parseFields(queryReconfigurationPlan, token);
        serializedString.erase(0, pos + lineDelimiter.length());
    }
    return queryReconfigurationPlan;
}

void QueryReconfigurationPlan::setQuerySubPlanIdsToStop(const std::vector<QuerySubPlanId> querySubPlanIdsToStop) {
    QueryReconfigurationPlan::querySubPlanIdsToStop = querySubPlanIdsToStop;
}
void QueryReconfigurationPlan::setQuerySubPlanIdsToStart(const std::vector<QuerySubPlanId> querySubPlanIdsToStart) {
    QueryReconfigurationPlan::querySubPlanIdsToStart = querySubPlanIdsToStart;
}
void QueryReconfigurationPlan::setQuerySubPlanIdsToReplace(
    const std::unordered_map<QuerySubPlanId, QuerySubPlanId> querySubPlanIdsToReplace) {
    QueryReconfigurationPlan::querySubPlanIdsToReplace = querySubPlanIdsToReplace;
}

QueryReconfigurationPlan::QueryReconfigurationPlan() {}

}// namespace NES
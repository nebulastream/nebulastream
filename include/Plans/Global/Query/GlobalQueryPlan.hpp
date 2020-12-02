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

#ifndef NES_GLOBALQUERYPLAN_HPP
#define NES_GLOBALQUERYPLAN_HPP

#include <Plans/Global/Query/GlobalQueryId.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Util/Logger.hpp>
#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <vector>

namespace NES {

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class GlobalQueryPlan;
typedef std::shared_ptr<GlobalQueryPlan> GlobalQueryPlanPtr;

class SourceLogicalOperatorNode;
typedef std::shared_ptr<SourceLogicalOperatorNode> SourceLogicalOperatorNodePtr;

class SinkLogicalOperatorNode;
typedef std::shared_ptr<SinkLogicalOperatorNode> SinkLogicalOperatorNodePtr;

class GlobalQueryMetaData;
typedef std::shared_ptr<GlobalQueryMetaData> GlobalQueryMetaDataPtr;

/**
 * @brief This class is responsible for storing all currently running and to be deployed QueryPlans in the NES system.
 * The QueryPlans included in the GlobalQueryPlan can be fused together and therefore each operator in GQP contains
 * information about the set of queries it belongs to. The QueryPlans are bound together by a dummy logical root operator.
 */
class GlobalQueryPlan {
  public:
    static GlobalQueryPlanPtr create();

    /**
     * @brief Add query plan to the global query plan
     * @param queryPlan : new query plan to be added.
     * @return: true if successful else false
     */
    bool addQueryPlan(QueryPlanPtr queryPlan);

    /**
     * @brief remove the operators belonging to the query with input query Id from the global query plan
     * @param queryId: the id of the query whose operators need to be removed
     */
    void removeQuery(QueryId queryId);

    /**
     * @brief This method will remove all deployed empty metadata information
     */
    void removeEmptyMetaData();

    /**
     * @brief Get all newly added Global Query Nodes with NodeType operators
     * @tparam NodeType: type of logical operator
     * @return vector of global query nodes
     */
    template<class T>
    std::vector<GlobalQueryNodePtr> getAllNewGlobalQueryNodesWithOperatorType() {
        NES_DEBUG("GlobalQueryPlan: Get all New Global query nodes with specific logical operators");
        std::vector<GlobalQueryNodePtr> vector = getAllGlobalQueryNodesWithOperatorType<T>();
        NES_DEBUG("GlobalQueryPlan: filter all pre-existing Global query nodes");
        std::remove_if(vector.begin(), vector.end(), [](GlobalQueryNodePtr globalQueryNode) {
            return globalQueryNode->hasNewUpdate();
        });
        return vector;
    }

    /**
     * @brief Get all Global Query Nodes with NodeType operators
     * @tparam NodeType: type of logical operator
     * @return vector of global query nodes
     */
    template<class T>
    std::vector<GlobalQueryNodePtr> getAllGlobalQueryNodesWithOperatorType() {
        NES_DEBUG("GlobalQueryPlan: Get all Global query nodes with specific logical operators");
        std::vector<GlobalQueryNodePtr> vector;
        for (const auto& childGlobalQueryNode : root->getChildren()) {
            childGlobalQueryNode->as<GlobalQueryNode>()->getNodesWithTypeHelper<T>(vector);
        }
        return vector;
    }

    /**
     * @brief Update the existing Global query meta data by re-grouping the queries
     * @return true if successful
     */
    bool updateGlobalQueryMetaDataMap();

    /**
     * @brief Get the all the Query Meta Data to be deployed
     * @return vector of global query meta data to be deployed
     */
    std::vector<GlobalQueryMetaDataPtr> getGlobalQueryMetaDataToDeploy();

    /**
     * @brief Get all global query metadata information
     * @return vector of global query meta data
     */
    std::vector<GlobalQueryMetaDataPtr> getAllGlobalQueryMetaData();

    /**
     * @brief Get the global query id for the query
     * @param queryId: the original query id
     * @return the corresponding global query id
     */
    GlobalQueryId getGlobalQueryIdForQuery(QueryId queryId);

  private:
    GlobalQueryPlan();

    /**
     * @brief Get next free node id
     * @return next free id
     */
    uint64_t getNextFreeId();

    /**
     * @brief Check if constructed metadata is still valid or not.
     * @return true if successful
     */
    bool checkMetaDataValidity();

    /**
     * @brief Update the global query meta data information
     * @param globalQueryMetaData: the global query metadata to be updated
     * @return true if successful
     */
    bool updateGlobalQueryMetadata(GlobalQueryMetaDataPtr globalQueryMetaData);

    uint64_t freeGlobalQueryNodeId;
    GlobalQueryNodePtr root;
//    std::map<QueryId, std::vector<GlobalQueryNodePtr>> queryIdToGlobalQueryNodeMap;
    std::map<GlobalQueryId, GlobalQueryMetaDataPtr> globalQueryIdToMetaDataMap;
    std::map<QueryId, GlobalQueryId> queryIdToGlobalQueryIdMap;
};
}// namespace NES
#endif//NES_GLOBALQUERYPLAN_HPP

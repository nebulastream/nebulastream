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

#include <Serialization/OperatorSerializationUtil.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableQueryPlan.pb.h>
#include <Plans/LogicalPlan.hpp>

namespace NES
{

void QueryPlanSerializationUtil::serializeQueryPlan(
    const LogicalPlan& queryPlan, SerializableQueryPlan* serializableQueryPlan, bool isClientOriginated)
{
    NES_DEBUG("QueryPlanSerializationUtil: serializing query plan {}", queryPlan.toString());
    auto rootOperators = queryPlan.rootOperators;

    ///Serialize Query Plan operators
    auto& serializedOperatorMap = *serializableQueryPlan->mutable_operatormap();
    /*for (auto itr = BFSRange<LogicalOperator>(queryPlan))
    {
        auto visitingOp = NES::Util::as<LogicalOperator>(*itr);
        if (serializedOperatorMap.find(visitingOp->id.getRawValue()) != serializedOperatorMap.end())
        {
            /// skip rest of the steps as the operator is already serialized
            continue;
        }
        NES_TRACE("QueryPlan: Inserting operator in collection of already visited node.");
        SerializableOperator serializeOperator = OperatorSerializationUtil::serializeOperator(visitingOp);
        serializedOperatorMap[visitingOp->id.getRawValue()] = serializeOperator;
    }*/

    ///Serialize the root operator ids
    for (const auto& rootOperator : rootOperators)
    {
        auto rootOperatorId = rootOperator.getId();
        serializableQueryPlan->add_rootoperatorids(rootOperatorId.getRawValue());
    }

    if (!isClientOriginated)
    {
        ///Serialize the sub query plan and query plan id
        NES_TRACE("QueryPlanSerializationUtil: serializing the Query sub plan id and query id");
        serializableQueryPlan->set_queryid(queryPlan.getQueryId().getRawValue());
    }
}

LogicalPlan QueryPlanSerializationUtil::deserializeQueryPlan(const SerializableQueryPlan* serializedQueryPlan)
{
    std::vector<LogicalOperator> rootOperators;
    std::map<uint64_t, LogicalOperator> operatorIdToOperatorMap;

    ///Deserialize all operators in the operator map
    for (const auto& operatorIdAndSerializedOperator : serializedQueryPlan->operatormap())
    {
        const auto& serializedOperator = operatorIdAndSerializedOperator.second;
        operatorIdToOperatorMap.emplace(serializedOperator.operatorid(),OperatorSerializationUtil::deserializeOperator(serializedOperator));
    }

    ///Add deserialized children
    for (const auto& operatorIdAndSerializedOperator : serializedQueryPlan->operatormap())
    {
        const auto& serializedOperator = operatorIdAndSerializedOperator.second;
        for (auto childId : serializedOperator.childrenids())
        {
            auto child = operatorIdToOperatorMap.find(childId);
            if (child != operatorIdToOperatorMap.end()) {
                auto it = operatorIdToOperatorMap.find(serializedOperator.operatorid());
                if (it != operatorIdToOperatorMap.end()) {
                    it->second = it->second.withChildren({ child->second });
                }
            }
        }
    }

    ///add root operators
    for (auto rootOperatorId : serializedQueryPlan->rootoperatorids())
    {
        auto root = operatorIdToOperatorMap.find(rootOperatorId);
        if (root != operatorIdToOperatorMap.end())
        {
            rootOperators.emplace_back(root->second);
        }
    }

    ///set properties of the query plan
    QueryId queryId = INVALID_QUERY_ID;

    if (serializedQueryPlan->has_queryid())
    {
        queryId = QueryId(serializedQueryPlan->queryid());
    }

    return LogicalPlan(queryId, std::move(rootOperators));
}
}

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

#include <Serialization/QueryPlanSerializationUtil.hpp>

#include <functional>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Serialization/OperatorSerializationUtil.hpp>
#include <Serialization/TraitSetSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableQueryPlan.pb.h>
#include <from_current.hpp>

namespace NES
{

SerializableQueryPlan QueryPlanSerializationUtil::serializeQueryPlan(const LogicalPlan& queryPlan)
{
    INVARIANT(queryPlan.getRootOperators().size() == 1, "Query plan should currently have only one root operator");
    auto rootOperator = queryPlan.getRootOperators().front();

    SerializableQueryPlan serializableQueryPlan;
    serializableQueryPlan.set_originalsql(queryPlan.getOriginalSql());
    /// Serialize Query Plan operators
    std::set<OperatorId> alreadySerialized;
    for (auto itr : BFSRange(rootOperator))
    {
        if (alreadySerialized.contains(itr.getId()))
        {
            /// Skip rest of the steps as the operator is already serialized
            continue;
        }
        alreadySerialized.insert(itr.getId());
        NES_TRACE("QueryPlan: Inserting operator in collection of already visited node.");
        auto* sOp = serializableQueryPlan.add_operators();
        itr.serialize(*sOp);
        TraitSetSerializationUtil::serialize(itr.getTraitSet(), sOp->mutable_trait_set());
    }

    /// Serialize the root operator ids
    auto rootOperatorId = rootOperator.getId();
    serializableQueryPlan.add_rootoperatorids(rootOperatorId.getRawValue());
    return serializableQueryPlan;
}

LogicalPlan QueryPlanSerializationUtil::deserializeQueryPlan(const SerializableQueryPlan& serializedQueryPlan)
{
    std::vector<Exception> deserializeExceptions;

    /// 1) Deserialize all operators into a map
    std::unordered_map<OperatorId::Underlying, LogicalOperator> baseOps;
    std::unordered_map<OperatorId::Underlying, std::vector<OperatorId::Underlying>> baseChildren;
    for (const auto& serializedOp : serializedQueryPlan.operators())
    {
        CPPTRACE_TRY
        {
            const auto operatorId = serializedOp.operator_id();
            auto [_, inserted] = baseOps.emplace(operatorId, OperatorSerializationUtil::deserializeOperator(serializedOp));
            if (!inserted)
            {
                throw CannotDeserialize("Duplicate operator id in {}", serializedQueryPlan.DebugString());
            }
            auto& opChildren = baseChildren[operatorId];
            opChildren.reserve(serializedOp.children_ids_size());
            for (auto child : serializedOp.children_ids())
            {
                opChildren.push_back(child);
            }
        }
        CPPTRACE_CATCH(...)
        {
            deserializeExceptions.push_back(wrapExternalException());
        }
    }

    if (!deserializeExceptions.empty())
    {
        std::string msgs;
        for (auto& deserExc : deserializeExceptions)
        {
            msgs += '\n';
            msgs += deserExc.what();
            msgs += deserExc.trace().to_string(true);
        }
        throw CannotDeserialize(
            "Deserialization of {} out of {} operators failed! Encountered Errors:{}",
            deserializeExceptions.size(),
            serializedQueryPlan.operators_size(),
            msgs);
    }

    /// 2) Recursive builder to attach all children
    std::unordered_map<OperatorId::Underlying, LogicalOperator> builtOps;
    std::function<LogicalOperator(OperatorId::Underlying, const std::set<OperatorId::Underlying>&)> build
        = [&](OperatorId::Underlying id, const std::set<OperatorId::Underlying>& ancestors) -> LogicalOperator
    {
        if (const auto memoIt = builtOps.find(id); memoIt != builtOps.end())
        {
            return memoIt->second;
        }
        const auto baseIt = baseOps.find(id);

        if (baseIt == baseOps.end())
        {
            throw CannotDeserialize("Unknown operator id: {}", id);
        }
        const LogicalOperator op = baseIt->second;

        std::vector<LogicalOperator> children;
        auto anc = ancestors;
        anc.insert(id);
        for (const auto childId : baseChildren.at(id))
        {
            if (ancestors.contains(childId))
            {
                throw CannotDeserialize("Cycle in operator graph! Operator {} has operator {} as child and ancestor!", id, childId);
            }
            children.push_back(build(childId, anc));
        }

        LogicalOperator withKids = op.withChildren(std::move(children)).withOperatorId(op.getId());
        builtOps.emplace(id, withKids);
        return withKids;
    };

    /// 3) Build root-operators
    if (serializedQueryPlan.rootoperatorids().empty())
    {
        throw CannotDeserialize("Query Plan has no root operator(s)!");
    }
    std::vector<LogicalOperator> rootOperators;
    for (auto rootId : serializedQueryPlan.rootoperatorids())
    {
        rootOperators.push_back(build(rootId, {}));
    }

    if (rootOperators.size() != 1)
    {
        throw CannotDeserialize("Plan contains multiple root operators!");
    }

    auto sink = rootOperators.at(0).tryGetAs<SinkLogicalOperator>();
    if (!sink)
    {
        throw CannotDeserialize(
            "Plan root has to be a source, but got {} from\n{}", rootOperators.at(0), serializedQueryPlan.DebugString());
    }

    if (sink->getChildren().empty())
    {
        throw CannotDeserialize("Sink has no children! From\n{}", serializedQueryPlan.DebugString());
    }

    if (not sink.value()->getSinkDescriptor())
    {
        throw CannotDeserialize("Sink has no descriptor!");
    }

    /// 4) Finalize plan
    auto queryId = INVALID_QUERY_ID;
    if (serializedQueryPlan.has_queryid())
    {
        queryId = QueryId(serializedQueryPlan.queryid());
    }
    std::string originalSql;
    if (serializedQueryPlan.has_originalsql())
    {
        originalSql = serializedQueryPlan.originalsql();
    }
    return LogicalPlan(queryId, std::move(rootOperators), std::move(originalSql));
}
}

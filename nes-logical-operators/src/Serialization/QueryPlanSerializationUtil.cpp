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
#include <memory>
#include <ranges>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Reflection.hpp>
#include <rfl/Generic.hpp>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>
#include <ErrorHandling.hpp>
#include <SerializableQueryId.pb.h>
#include <SerializableQueryPlan.pb.h>
#include <from_current.hpp>

#include <Serialization/OperatorMapping.hpp>

namespace NES
{

SerializableQueryPlan QueryPlanSerializationUtil::serializeQueryPlan(const LogicalPlan& queryPlan)
{
    INVARIANT(queryPlan.getRootOperators().size() == 1, "Query plan should currently have only one root operator");
    auto rootOperator = queryPlan.getRootOperators().front();

    SerializableQueryPlan serializableQueryPlan;
    if (queryPlan.getQueryId().isValid())
    {
        *serializableQueryPlan.mutable_queryid() = serializeQueryId(queryPlan.getQueryId());
    }
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

        auto childrenIds = itr->getChildren() | std::views::transform([](const auto& child) { return child->getOperatorId(); })
            | std::ranges::to<std::vector>();
        const ReflectedOperator reflectedOperator{
            .type = std::string{itr->getName()},
            .operatorId = itr->getOperatorId(),
            .childrenIds = std::move(childrenIds),
            .config = itr->reflect(),
            .traitSet = itr->getTraitSet()};

        const auto serializedString = rfl::json::write(*reflect(reflectedOperator));
        serializableQueryPlan.add_reflectedoperators(serializedString);
    }

    /// Serialize the root operator ids
    auto rootOperatorId = rootOperator.getId();
    serializableQueryPlan.add_rootoperatorids(rootOperatorId.getRawValue());
    return serializableQueryPlan;
}

LogicalPlan QueryPlanSerializationUtil::deserializeQueryPlan(const SerializableQueryPlan& serializedQueryPlan)
{
    std::vector<Exception> deserializeExceptions;

    std::unordered_map<OperatorId, ReflectedOperator> reflectedOperators;
    for (const auto& reflectedOp : serializedQueryPlan.reflectedoperators())
    {
        CPPTRACE_TRY
        {
            auto parsed = rfl::json::read<rfl::Generic>(reflectedOp);
            if (!parsed.has_value())
            {
                throw CannotDeserialize(parsed.error().what());
            }

            auto reflectedOperator = ReflectionContext{}.unreflect<ReflectedOperator>(*parsed);
            reflectedOperators.emplace(reflectedOperator.operatorId, reflectedOperator);
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
            serializedQueryPlan.reflectedoperators_size(),
            msgs);
    }

    const auto reflectedPlan = std::make_shared<ReflectedPlan>(reflectedOperators);
    const auto operatorMapping = std::static_pointer_cast<OperatorMapping>(reflectedPlan);
    const auto reflectionContext = ReflectionContext{reflectedPlan, operatorMapping};

    /// 3) Build root-operators
    if (serializedQueryPlan.rootoperatorids().empty())
    {
        throw CannotDeserialize("Query Plan has no root operator(s)!");
    }
    std::vector<LogicalOperator> rootOperators;
    for (auto rootId : serializedQueryPlan.rootoperatorids())
    {
        if (const auto foundOperator = reflectedOperators.find(OperatorId{rootId}); foundOperator != reflectedOperators.end())
        {
            if (auto buildOp = reflectedPlan->getOperator(OperatorId{rootId}, reflectionContext))
            {
                rootOperators.push_back(std::move(buildOp).value());
            }
            else
            {
                throw CannotDeserialize("Could not create root operator {}", rootId);
            }
        }
    }

    if (rootOperators.size() != 1)
    {
        throw CannotDeserialize("Plan contains multiple root operators!");
    }

    auto sinkOpt = rootOperators.at(0).tryGetAs<SinkLogicalOperator>();
    if (!sinkOpt)
    {
        throw CannotDeserialize("Plan root has to be a sink, but got {} from\n{}", rootOperators.at(0), serializedQueryPlan.DebugString());
    }
    auto sink = std::move(sinkOpt).value();

    if (sink->getChildren().empty())
    {
        throw CannotDeserialize("Sink has no children! From\n{}", serializedQueryPlan.DebugString());
    }

    if (not sink->getSinkDescriptor())
    {
        throw CannotDeserialize("Sink has no descriptor!");
    }

    rootOperators = std::vector<LogicalOperator>{sink};

    /// 4) Finalize plan
    auto queryId = INVALID_QUERY_ID;
    if (serializedQueryPlan.has_queryid())
    {
        queryId = deserializeQueryId(serializedQueryPlan.queryid());
    }
    return LogicalPlan(queryId, std::move(rootOperators));
}

NES::SerializableQueryId QueryPlanSerializationUtil::serializeQueryId(const QueryId& queryId)
{
    NES::SerializableQueryId proto;
    proto.set_local_query_id(queryId.getLocalQueryId().getRawValue());
    proto.set_distributed_query_id(queryId.getDistributedQueryId().getRawValue());
    return proto;
}

QueryId QueryPlanSerializationUtil::deserializeQueryId(const NES::SerializableQueryId& proto)
{
    auto localId = LocalQueryId(proto.local_query_id());
    auto distributedId = DistributedQueryId(proto.distributed_query_id());
    const bool hasLocal = localId != INVALID_LOCAL_QUERY_ID;
    const bool hasDistributed = distributedId != DistributedQueryId(DistributedQueryId::INVALID);
    if (hasLocal && hasDistributed)
    {
        return QueryId::create(localId, distributedId);
    }
    if (hasLocal)
    {
        return QueryId::createLocal(localId);
    }
    if (hasDistributed)
    {
        return QueryId::createDistributed(distributedId);
    }
    return QueryId::invalid();
}
}

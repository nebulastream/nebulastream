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

#include <utility>
#include <API/AttributeField.hpp>
#include <API/Functions/Functions.hpp>
#include <API/Functions/LogicalFunctions.hpp>
#include <API/Query.hpp>
#include <API/WindowedQuery.hpp>
#include <API/Windowing.hpp>
#include <Functions/FieldAssignmentFunctionNode.hpp>
#include <Functions/FieldRenameFunctionNode.hpp>
#include <Functions/LogicalFunctions/EqualsFunctionNode.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

FunctionNodePtr getFunctionNodePtr(FunctionItem& functionItem)
{
    return functionItem.getFunctionNode();
}

JoinOperatorBuilder::Join Query::joinWith(const Query& subQueryRhs)
{
    return JoinOperatorBuilder::Join(subQueryRhs, *this);
}

NES::Experimental::BatchJoinOperatorBuilder::Join Query::batchJoinWith(const Query& subQueryRhs)
{
    return NES::Experimental::BatchJoinOperatorBuilder::Join(subQueryRhs, *this);
}

CEPOperatorBuilder::And Query::andWith(const Query& subQueryRhs)
{
    return CEPOperatorBuilder::And(subQueryRhs, *this);
}

CEPOperatorBuilder::Seq Query::seqWith(const Query& subQueryRhs)
{
    return CEPOperatorBuilder::Seq(subQueryRhs, *this);
}

CEPOperatorBuilder::Times Query::times(const uint64_t minOccurrences, const uint64_t maxOccurrences)
{
    return CEPOperatorBuilder::Times(minOccurrences, maxOccurrences, *this);
}

CEPOperatorBuilder::Times Query::times(const uint64_t occurrences)
{
    return CEPOperatorBuilder::Times(occurrences, *this);
}

CEPOperatorBuilder::Times Query::times()
{
    return CEPOperatorBuilder::Times(*this);
}

namespace JoinOperatorBuilder
{

JoinWhere Join::where(FunctionNodePtr joinFunction) const
{
    return JoinWhere(subQueryRhs, originalQuery, joinFunction);
}

Join::Join(const Query& subQueryRhs, Query& originalQuery) : subQueryRhs(subQueryRhs), originalQuery(originalQuery)
{
}

JoinWhere::JoinWhere(const Query& subQueryRhs, Query& originalQuery, FunctionNodePtr joinFunction)
    : subQueryRhs(subQueryRhs), originalQuery(originalQuery), joinFunctions(joinFunction)
{
}

Query& JoinWhere::window(const Windowing::WindowTypePtr& windowType) const
{
    return originalQuery.joinWith(subQueryRhs, joinFunctions, windowType); ///call original joinWith() function
}

} /// namespace JoinOperatorBuilder

namespace Experimental::BatchJoinOperatorBuilder
{

Join::Join(const Query& subQueryRhs, Query& originalQuery) : subQueryRhs(subQueryRhs), originalQuery(originalQuery)
{
}

Query& Join::where(const FunctionNodePtr joinFunction) const
{
    return originalQuery.batchJoinWith(subQueryRhs, joinFunction);
}

} /// namespace Experimental::BatchJoinOperatorBuilder

namespace CEPOperatorBuilder
{

And::And(const Query& subQueryRhs, Query& originalQuery) : subQueryRhs(const_cast<Query&>(subQueryRhs)), originalQuery(originalQuery)
{
    NES_DEBUG("Query: add map operator to andWith to add virtual key to originalQuery");
    ///here, we add artificial key attributes to the sources in order to reuse the join-logic later
    auto cepLeftKey = "cep_leftKey";
    auto cepRightKey = "cep_rightKey";
    ///next: map the attributes with value 1 to the left and right source
    originalQuery.map(Attribute(cepLeftKey) = 1);
    this->subQueryRhs.map(Attribute(cepRightKey) = 1);
    ///last, define the artificial attributes as key attributes
    NES_DEBUG("Query: add name cepLeftKey {}", cepLeftKey);
    NES_DEBUG("Query: add name cepRightKey {}", cepRightKey);
    joinFunction
        = FunctionItem(Attribute(cepLeftKey)).getFunctionNode() == FunctionItem(Attribute(cepRightKey)).getFunctionNode();
}

Query& And::window(const Windowing::WindowTypePtr& windowType) const
{
    return originalQuery.andWith(subQueryRhs, joinFunction, windowType); ///call original andWith() function
}

Seq::Seq(const Query& subQueryRhs, Query& originalQuery) : subQueryRhs(const_cast<Query&>(subQueryRhs)), originalQuery(originalQuery)
{
    NES_DEBUG("Query: add map operator to seqWith to add virtual key to originalQuery");
    ///here, we add artificial key attributes to the sources in order to reuse the join-logic later
    auto cepLeftKey = "cep_leftKey";
    auto cepRightKey = "cep_rightKey";
    ///next: map the attributes with value 1 to the left and right source
    originalQuery.map(Attribute(cepLeftKey) = 1);
    this->subQueryRhs.map(Attribute(cepRightKey) = 1);
    ///last, define the artificial attributes as key attributes
    joinFunction
        = FunctionItem(Attribute(cepLeftKey)).getFunctionNode() == FunctionItem(Attribute(cepRightKey)).getFunctionNode();
}

Query& Seq::window(const Windowing::WindowTypePtr& windowType) const
{
    NES_DEBUG("Sequence enters window function");
    auto timestamp
        = windowType->as<Windowing::TimeBasedWindowType>()->getTimeCharacteristic()->getField()->getName(); /// assume time-based windows
    std::string sourceNameLeft = originalQuery.getQueryPlan()->getSourceConsumed();
    std::string sourceNameRight = subQueryRhs.getQueryPlan()->getSourceConsumed();
    /// to guarantee a correct order of events by time (sequence) we need to identify the correct source and its timestamp
    /// in case of composed streams on the right branch
    if (sourceNameRight.find("_") != std::string::npos)
    {
        /// we find the most left source and use its timestamp for the filter constraint
        uint64_t posStart = sourceNameRight.find("_");
        uint64_t posEnd = sourceNameRight.find("_", posStart + 1);
        sourceNameRight = sourceNameRight.substr(posStart + 1, posEnd - 2) + "$" + timestamp;
    } /// in case the right branch only contains 1 source we can just use it
    else
    {
        sourceNameRight = sourceNameRight + "$" + timestamp;
    }
    /// in case of composed sources on the left branch
    if (sourceNameLeft.find("_") != std::string::npos)
    {
        /// we find the most right source and use its timestamp for the filter constraint
        uint64_t posStart = sourceNameLeft.find_last_of("_");
        sourceNameLeft = sourceNameLeft.substr(posStart + 1) + "$" + timestamp;
    } /// in case the left branch only contains 1 source we can just use it
    else
    {
        sourceNameLeft = sourceNameLeft + "$" + timestamp;
    }
    NES_DEBUG("FunctionItem for Left Source {}", sourceNameLeft);
    NES_DEBUG("FunctionItem for Right Source {}", sourceNameRight);
    return originalQuery.seqWith(subQueryRhs, joinFunction, windowType)
        .filter(Attribute(sourceNameLeft) < Attribute(sourceNameRight)); ///call original seqWith() function
}

Times::Times(const uint64_t minOccurrences, const uint64_t maxOccurrences, Query& originalQuery)
    : originalQuery(originalQuery), minOccurrences(minOccurrences), maxOccurrences(maxOccurrences), bounded(true)
{
    /// add a new count attribute to the schema which is later used to derive the number of occurrences
    originalQuery.map(Attribute("Count") = 1);
}

Times::Times(const uint64_t occurrences, Query& originalQuery)
    : originalQuery(originalQuery), minOccurrences(0), maxOccurrences(occurrences), bounded(true)
{
    /// add a new count attribute to the schema which is later used to derive the number of occurrences
    originalQuery.map(Attribute("Count") = 1);
}

Times::Times(Query& originalQuery) : originalQuery(originalQuery), minOccurrences(0), maxOccurrences(0), bounded(false)
{
    /// add a new count attribute to the schema which is later used to derive the number of occurrences
    originalQuery.map(Attribute("Count") = 1);
}

Query& Times::window(const Windowing::WindowTypePtr& windowType) const
{
    auto timestamp = windowType->as<Windowing::TimeBasedWindowType>()->getTimeCharacteristic()->getField()->getName();
    /// if no min and max occurrence is defined, apply count without filter
    if (!bounded)
    {
        return originalQuery.window(windowType).apply(API::Sum(Attribute("Count")), API::Max(Attribute(timestamp)));
    }
    else
    {
        /// if user passed 0 occurrences which is not wanted
        if ((minOccurrences == 0) && (maxOccurrences == 0))
        {
            NES_THROW_RUNTIME_ERROR("Number of occurrences must be at least 1.");
        }
        /// if min and/or max occurrence are defined, apply count without filter
        if (maxOccurrences == 0)
        {
            return originalQuery.window(windowType)
                .apply(API::Sum(Attribute("Count")), API::Max(Attribute(timestamp)))
                .filter(Attribute("Count") >= minOccurrences);
        }

        if (minOccurrences == 0)
        {
            return originalQuery.window(windowType)
                .apply(API::Sum(Attribute("Count")), API::Max(Attribute(timestamp)))
                .filter(Attribute("Count") == maxOccurrences);
        }

        return originalQuery.window(windowType)
            .apply(API::Sum(Attribute("Count")), API::Max(Attribute(timestamp)))
            .filter(Attribute("Count") >= minOccurrences && Attribute("Count") <= maxOccurrences);
    }

    return originalQuery;
}

} /// namespace CEPOperatorBuilder

Query::Query(QueryPlanPtr queryPlan) : queryPlan(std::move(queryPlan))
{
}

Query::Query(const Query& query) = default;

Query Query::from(const std::string& sourceName)
{
    NES_DEBUG("Query: create new Query with source {}", sourceName);
    auto queryPlan = QueryPlanBuilder::createQueryPlan(sourceName);
    return Query(queryPlan);
}

Query& Query::project(std::vector<FunctionNodePtr> functions)
{
    NES_DEBUG("Query: add projection to query");
    this->queryPlan = QueryPlanBuilder::addProjection(functions, this->queryPlan);
    return *this;
}

Query& Query::as(const std::string& newSourceName)
{
    NES_DEBUG("Query: add rename operator to query");
    this->queryPlan = QueryPlanBuilder::addRename(newSourceName, this->queryPlan);
    return *this;
}

Query& Query::unionWith(const Query& subQuery)
{
    NES_DEBUG("Query: unionWith the subQuery to current query");
    this->queryPlan = QueryPlanBuilder::addUnion(this->queryPlan, subQuery.getQueryPlan());
    return *this;
}

Query& Query::joinWith(const Query& subQueryRhs, FunctionNodePtr joinFunction, const Windowing::WindowTypePtr& windowType)
{
    Join::LogicalJoinDescriptor::JoinType joinType = identifyJoinType(joinFunction);
    this->queryPlan = QueryPlanBuilder::addJoin(this->queryPlan, subQueryRhs.getQueryPlan(), joinFunction, windowType, joinType);
    return *this;
}

Query& Query::batchJoinWith(const Query& subQueryRhs, FunctionNodePtr joinFunction)
{
    NES_DEBUG("Query: add Batch Join Operator to Query");
    if (joinFunction->as<EqualsFunctionNode>())
    {
        auto onProbeKey = joinFunction->as<BinaryFunctionNode>()->getLeft();
        auto onBuildKey = joinFunction->as<BinaryFunctionNode>()->getRight();

        this->queryPlan = QueryPlanBuilder::addBatchJoin(this->queryPlan, subQueryRhs.getQueryPlan(), onProbeKey, onBuildKey);
    }
    else
    {
        NES_THROW_RUNTIME_ERROR("Query:joinFunction has to be a EqualsFunctionNode");
    }
    return *this;
}

Query& Query::andWith(const Query& subQueryRhs, FunctionNodePtr joinFunction, const Windowing::WindowTypePtr& windowType)
{
    Join::LogicalJoinDescriptor::JoinType joinType = identifyJoinType(joinFunction);
    this->queryPlan = QueryPlanBuilder::addJoin(this->queryPlan, subQueryRhs.getQueryPlan(), joinFunction, windowType, joinType);
    return *this;
}

Query& Query::seqWith(const Query& subQueryRhs, FunctionNodePtr joinFunction, const Windowing::WindowTypePtr& windowType)
{
    Join::LogicalJoinDescriptor::JoinType joinType = identifyJoinType(joinFunction);
    this->queryPlan = QueryPlanBuilder::addJoin(this->queryPlan, subQueryRhs.getQueryPlan(), joinFunction, windowType, joinType);
    return *this;
}

Query& Query::orWith(const Query& subQueryRhs)
{
    NES_DEBUG("Query: finally we translate the OR into a union OP ");
    this->queryPlan = QueryPlanBuilder::addUnion(this->queryPlan, subQueryRhs.getQueryPlan());
    return *this;
}

Query& Query::filter(const FunctionNodePtr& filterFunction)
{
    NES_DEBUG("Query: add filter operator to query");
    this->queryPlan = QueryPlanBuilder::addFilter(filterFunction, this->queryPlan);
    return *this;
}

Query& Query::limit(const uint64_t limit)
{
    NES_DEBUG("Query: add limit operator to query");
    this->queryPlan = QueryPlanBuilder::addLimit(limit, this->queryPlan);
    return *this;
}
Query& Query::map(const FieldAssignmentFunctionNodePtr& mapExpression)
{
    NES_DEBUG("Query: add map operator to query");
    this->queryPlan = QueryPlanBuilder::addMap(mapFunction, this->queryPlan);
    return *this;
}

Query& Query::inferModel(
    const std::string model,
    const std::initializer_list<FunctionItem> inputFields,
    const std::initializer_list<FunctionItem> outputFields)
{
    NES_DEBUG("Query: add map inferModel to query");
    auto inputFieldVector = std::vector(inputFields);
    auto outputFieldVector = std::vector(outputFields);
    std::vector<FunctionNodePtr> inputFieldsPtr;
    std::vector<FunctionNodePtr> outputFieldsPtr;
    for (const auto& inputField : inputFieldVector)
    {
        inputFieldsPtr.push_back(inputField.getFunctionNode());
    }
    for (const auto& outputField : outputFieldVector)
    {
        outputFieldsPtr.push_back(outputField.getFunctionNode());
    }

    OperatorPtr op = LogicalOperatorFactory::createInferModelOperator(model, inputFieldsPtr, outputFieldsPtr);
    NES_DEBUG("Query::inferModel: Current Operator: {}", op->toString());
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::sink(const SinkDescriptorPtr sinkDescriptor, WorkerId workerId)
{
    NES_DEBUG("Query: add sink operator to query");
    this->queryPlan = QueryPlanBuilder::addSink(this->queryPlan, sinkDescriptor, workerId);
    return *this;
}

Query& Query::assignWatermark(const Windowing::WatermarkStrategyDescriptorPtr& watermarkStrategyDescriptor)
{
    NES_DEBUG("Query: add assignWatermark operator to query");
    this->queryPlan = QueryPlanBuilder::assignWatermark(this->queryPlan, watermarkStrategyDescriptor);
    return *this;
}

QueryPlanPtr Query::getQueryPlan() const
{
    return queryPlan;
}

///
Join::LogicalJoinDescriptor::JoinType Query::identifyJoinType(FunctionNodePtr joinFunction)
{
    NES_DEBUG("Query: identify Join Type; default: CARTESIAN PRODUCT");
    auto joinType = Join::LogicalJoinDescriptor::JoinType::CARTESIAN_PRODUCT;
    NES_DEBUG("Query: Iterate over all FunctionNode to check join field.");
    std::unordered_set<std::shared_ptr<BinaryFunctionNode>> visitedFunctions;
    auto bfsIterator = BreadthFirstNodeIterator(joinFunction);
    for (auto itr = bfsIterator.begin(); itr != BreadthFirstNodeIterator::end(); ++itr)
    {
        if ((*itr)->instanceOf<BinaryFunctionNode>())
        {
            auto visitingOp = (*itr)->as<BinaryFunctionNode>();
            if (visitedFunctions.contains(visitingOp))
            {
                /// skip rest of the steps as the node found in already visited node list
                continue;
            }
            else
            {
                visitedFunctions.insert(visitingOp);
                if ((*itr)->instanceOf<EqualsFunctionNode>())
                {
                    NES_DEBUG("Query: identify Join Type: INNER JOIN");
                    joinType = Join::LogicalJoinDescriptor::JoinType::INNER_JOIN;
                    break;
                }
            }
        }
    }
    return joinType;
}

} /// namespace NES

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

#include <memory>
#include <utility>
#include <vector>
#include <API/Functions/Functions.hpp>
#include <API/Functions/LogicalFunctions.hpp>
#include <API/Query.hpp>
#include <API/WindowedQuery.hpp>
#include <API/Windowing.hpp>
#include <Functions/ConstantExpression.hpp>
#include <Functions/Expression.hpp>
#include <Functions/FunctionExpression.hpp>
#include <Functions/WellKnownFunctions.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkStrategyDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Types/WindowType.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <WellKnownDataTypes.hpp>


namespace NES
{

ExpressionValue getNodeFunction(FunctionItem& functionItem)
{
    return functionItem.getNodeFunction();
}

JoinOperatorBuilder::Join Query::joinWith(const Query& subQueryRhs)
{
    return JoinOperatorBuilder::Join(subQueryRhs, *this);
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
JoinWhere Join::where(ExpressionValue joinFunction) const
{
    return JoinWhere(subQueryRhs, originalQuery, joinFunction);
}

Join::Join(const Query& subQueryRhs, Query& originalQuery) : subQueryRhs(subQueryRhs), originalQuery(originalQuery)
{
}

JoinWhere::JoinWhere(const Query& subQueryRhs, Query& originalQuery, ExpressionValue joinFunction)
    : subQueryRhs(subQueryRhs), originalQuery(originalQuery), joinFunctions(std::move(joinFunction))
{
}

Query& JoinWhere::window(const std::shared_ptr<Windowing::WindowType>& windowType) const
{
    return originalQuery.joinWith(subQueryRhs, joinFunctions, windowType); ///call original joinWith() function
}

}

namespace CEPOperatorBuilder
{

And::And(const Query& subQueryRhs, Query& originalQuery)
    : subQueryRhs(const_cast<Query&>(subQueryRhs))
    , originalQuery(originalQuery)
    , joinFunction(make_expression<ConstantExpression>("UNDEFINED"))
{
    NES_DEBUG("Query: add map operator to andWith to add virtual key to originalQuery");
    ///here, we add artificial key attributes to the sources in order to reuse the join-logic later
    auto cepLeftKey = "cep_leftKey";
    auto cepRightKey = "cep_rightKey";
    ///next: map the attributes with value 1 to the left and right source
    originalQuery.map(Schema::Identifier(cepLeftKey), FunctionItem(1));
    this->subQueryRhs.map(Schema::Identifier(cepRightKey), FunctionItem(1));
    ///last, define the artificial attributes as key attributes
    NES_DEBUG("Query: add name cepLeftKey {}", cepLeftKey);
    NES_DEBUG("Query: add name cepRightKey {}", cepRightKey);
    joinFunction = make_expression<FunctionExpression>(
        {FunctionItem(Attribute(cepLeftKey)).getNodeFunction(), FunctionItem(Attribute(cepRightKey)).getNodeFunction()}, WellKnown::Equal);
}

Query& And::window(const std::shared_ptr<Windowing::WindowType>& windowType) const
{
    return originalQuery.andWith(subQueryRhs, joinFunction, windowType); ///call original andWith() function
}

Seq::Seq(const Query& subQueryRhs, Query& originalQuery)
    : subQueryRhs(const_cast<Query&>(subQueryRhs))
    , originalQuery(originalQuery)
    , joinFunction(make_expression<ConstantExpression>("UNDEFINED"))
{
    NES_DEBUG("Query: add map operator to seqWith to add virtual key to originalQuery");
    ///here, we add artificial key attributes to the sources in order to reuse the join-logic later
    auto cepLeftKey = "cep_leftKey";
    auto cepRightKey = "cep_rightKey";
    ///next: map the attributes with value 1 to the left and right source
    originalQuery.map(Schema::Identifier(cepLeftKey), FunctionItem(1));
    this->subQueryRhs.map(Schema::Identifier(cepRightKey), FunctionItem(1));
    ///last, define the artificial attributes as key attributes

    joinFunction = make_expression<FunctionExpression>(
        {FunctionItem(Attribute(cepLeftKey)).getNodeFunction(), FunctionItem(Attribute(cepRightKey)).getNodeFunction()}, WellKnown::Equal);
}

Query& Seq::window(const std::shared_ptr<Windowing::WindowType>& windowType) const
{
    NES_DEBUG("Sequence enters window function");
    auto timestamp = Util::as<Windowing::TimeBasedWindowType>(windowType)->getTimeCharacteristic()->value; /// assume time-based windows
    std::string sourceNameLeft = originalQuery.getQueryPlan()->getSourceConsumed();
    std::string sourceNameRight = subQueryRhs.getQueryPlan()->getSourceConsumed();
    /// to guarantee a correct order of events by time (sequence) we need to identify the correct source and its timestamp
    /// in case of composed streams on the right branch
    if (sourceNameRight.find('_') != std::string::npos)
    {
        /// we find the most left source and use its timestamp for the filter constraint
        uint64_t posStart = sourceNameRight.find('_');
        uint64_t posEnd = sourceNameRight.find('_', posStart + 1);
        sourceNameRight = sourceNameRight.substr(posStart + 1, posEnd - 2) + "$" + format_as(timestamp);
    } /// in case the right branch only contains 1 source we can just use it
    else
    {
        sourceNameRight = sourceNameRight + "$" + format_as(timestamp);
    }
    /// in case of composed sources on the left branch
    if (sourceNameLeft.find('_') != std::string::npos)
    {
        /// we find the most right source and use its timestamp for the filter constraint
        uint64_t posStart = sourceNameLeft.find_last_of('_');
        sourceNameLeft = sourceNameLeft.substr(posStart + 1) + "$" + format_as(timestamp);
    } /// in case the left branch only contains 1 source we can just use it
    else
    {
        sourceNameLeft = sourceNameLeft + "$" + format_as(timestamp);
    }
    NES_DEBUG("FunctionItem for Left Source {}", sourceNameLeft);
    NES_DEBUG("FunctionItem for Right Source {}", sourceNameRight);
    return originalQuery.seqWith(subQueryRhs, joinFunction, windowType)
        .selection(Attribute(sourceNameLeft) < Attribute(sourceNameRight)); ///call original seqWith() function
}

Times::Times(const uint64_t minOccurrences, const uint64_t maxOccurrences, Query& originalQuery)
    : originalQuery(originalQuery), minOccurrences(minOccurrences), maxOccurrences(maxOccurrences), bounded(true)
{
    /// add a new count attribute to the schema which is later used to derive the number of occurrences
    originalQuery.map(Schema::Identifier("Count"), FunctionItem(1));
}

Times::Times(const uint64_t occurrences, Query& originalQuery)
    : originalQuery(originalQuery), minOccurrences(0), maxOccurrences(occurrences), bounded(true)
{
    /// add a new count attribute to the schema which is later used to derive the number of occurrences
    originalQuery.map(Schema::Identifier("Count"), FunctionItem(1));
}

Times::Times(Query& originalQuery) : originalQuery(originalQuery), minOccurrences(0), maxOccurrences(0), bounded(false)
{
    /// add a new count attribute to the schema which is later used to derive the number of occurrences
    originalQuery.map(Schema::Identifier("Count"), FunctionItem(1));
}

Query& Times::window(const std::shared_ptr<Windowing::WindowType>& windowType) const
{
    auto timestamp = Util::as<Windowing::TimeBasedWindowType>(windowType)->getTimeCharacteristic()->value;
    /// if no min and max occurrence is defined, apply count without filter
    if (!bounded)
    {
        return originalQuery.window(windowType).apply(API::Sum(Attribute("Count")), API::Max(timestamp));
    }
    else
    {
        /// if user passed 0 occurrences which is not wanted
        PRECONDITION(!(minOccurrences == 0 && maxOccurrences == 0), "Number of occurrences must be at least 1.");
        /// if min and/or max occurrence are defined, apply count without filter
        if (maxOccurrences == 0)
        {
            return originalQuery.window(windowType)
                .apply(API::Sum(Attribute("Count")), API::Max(timestamp))
                .selection(Attribute("Count") >= minOccurrences);
        }

        if (minOccurrences == 0)
        {
            return originalQuery.window(windowType)
                .apply(API::Sum(Attribute("Count")), API::Max(timestamp))
                .selection(Attribute("Count") == maxOccurrences);
        }

        return originalQuery.window(windowType)
            .apply(API::Sum(Attribute("Count")), API::Max(timestamp))
            .selection(Attribute("Count") >= minOccurrences && Attribute("Count") <= maxOccurrences);
    }

    return originalQuery;
}

}

Query::Query(std::shared_ptr<QueryPlan> queryPlan) : queryPlan(std::move(queryPlan))
{
}

Query::Query(const Query& query) = default;

Query Query::from(const std::string& logicalSourceName)
{
    NES_DEBUG("Query: create new Query with source {}", logicalSourceName);
    auto queryPlan = QueryPlanBuilder::createQueryPlan(logicalSourceName);
    return Query(queryPlan);
}

Query& Query::project(const std::vector<ExpressionValue>& functions)
{
    NES_DEBUG("Query: add projection to query");
    this->queryPlan = QueryPlanBuilder::addProjection(functions, this->queryPlan);
    return *this;
}

Query& Query::unionWith(const Query& subQuery)
{
    NES_DEBUG("Query: unionWith the subQuery to current query");
    this->queryPlan = QueryPlanBuilder::addUnion(this->queryPlan, subQuery.getQueryPlan());
    return *this;
}

Query& Query::joinWith(const Query& subQueryRhs, ExpressionValue joinFunction, const std::shared_ptr<Windowing::WindowType>& windowType)
{
    Join::LogicalJoinDescriptor::JoinType joinType = identifyJoinType(joinFunction);
    this->queryPlan = QueryPlanBuilder::addJoin(this->queryPlan, subQueryRhs.getQueryPlan(), joinFunction, windowType, joinType);
    return *this;
}

Query& Query::andWith(const Query& subQueryRhs, ExpressionValue joinFunction, const std::shared_ptr<Windowing::WindowType>& windowType)
{
    Join::LogicalJoinDescriptor::JoinType joinType = identifyJoinType(joinFunction);
    this->queryPlan = QueryPlanBuilder::addJoin(this->queryPlan, subQueryRhs.getQueryPlan(), joinFunction, windowType, joinType);
    return *this;
}

Query& Query::seqWith(const Query& subQueryRhs, ExpressionValue joinFunction, const std::shared_ptr<Windowing::WindowType>& windowType)
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

Query& Query::selection(ExpressionValue selectionFunction)
{
    NES_DEBUG("Query: add selection operator to query");
    this->queryPlan = QueryPlanBuilder::addSelection(selectionFunction, this->queryPlan);
    return *this;
}

Query& Query::limit(const uint64_t limit)
{
    NES_DEBUG("Query: add limit operator to query");
    this->queryPlan = QueryPlanBuilder::addLimit(limit, this->queryPlan);
    return *this;
}
Query& Query::map(std::string asField, ExpressionValue value)
{
    return map(Schema::Identifier{.name = std::move(asField), .table = {}}, std::move(value));
}
Query& Query::map(Schema::Identifier asField, ExpressionValue value)
{
    NES_DEBUG("Query: add map operator to query");
    this->queryPlan = QueryPlanBuilder::addMap(std::move(asField), std::move(value), this->queryPlan);
    return *this;
}

Query& Query::sink(std::string sinkName, WorkerId workerId)
{
    NES_DEBUG("Query: add sink operator to query");
    this->queryPlan = QueryPlanBuilder::addSink(std::move(sinkName), this->queryPlan, workerId);
    return *this;
}

Query& Query::assignWatermark(const std::shared_ptr<Windowing::WatermarkStrategyDescriptor>& watermarkStrategyDescriptor)
{
    NES_DEBUG("Query: add assignWatermark operator to query");
    this->queryPlan = QueryPlanBuilder::assignWatermark(this->queryPlan, watermarkStrategyDescriptor);
    return *this;
}

std::shared_ptr<QueryPlan> Query::getQueryPlan() const
{
    return queryPlan;
}

///
Join::LogicalJoinDescriptor::JoinType Query::identifyJoinType(ExpressionValue)
{
    return Join::LogicalJoinDescriptor::JoinType::INNER_JOIN;
}

}

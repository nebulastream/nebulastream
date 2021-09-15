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

#ifndef API_QUERY_H
#define API_QUERY_H

#include <API/Expressions/ArithmeticalExpressions.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <API/Schema.hpp>
#include <API/Windowing.hpp>

#include <API/Expressions/Expressions.hpp>
#include <API/WindowedQuery.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Sources/DataSource.hpp>

#ifdef ENABLE_KAFKA_BUILD
#include <cppkafka/configuration.h>
#endif// KAFKASINK_HPP
#include <string>

namespace NES {

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class ExpressionNode;
using ExpressionNodePtr = std::shared_ptr<ExpressionNode>;

class SourceLogicalOperatorNode;
using SourceLogicalOperatorNodePtr = std::shared_ptr<SourceLogicalOperatorNode>;

class SinkLogicalOperatorNode;
using SinkLogicalOperatorNodePtr = std::shared_ptr<SinkLogicalOperatorNode>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class WindowType;
using WindowTypePtr = std::shared_ptr<WindowType>;

class WindowAggregationDescriptor;
using WindowAggregationPtr = std::shared_ptr<WindowAggregationDescriptor>;

using namespace NES::API;
using namespace NES::Windowing;

static constexpr uint64_t defaultTriggerTimeInMs = 1000;

namespace JoinOperatorBuilder {

class JoinWhere;
class JoinCondition;

class Join {
  public:
    /**
     * @brief Constructor. Initialises always subQueryRhs and original Query
     * @param subQueryRhs
     * @param originalQuery
     */
    Join(const Query& subQueryRhs, Query& originalQuery);

    /**
     * @brief sets the left key item, after that it can be compared with the function implemented in Condition
     * @param onLeftKey
     * @return object of type JoinWhere on which equalsTo function is defined and can be called.
     */
    [[nodiscard]] JoinWhere where(const ExpressionItem& onLeftKey) const;

  private:
    const Query& subQueryRhs;
    Query& originalQuery;
};

class JoinWhere {
  public:
    /**
     * @brief Constructor. Initialises always subQueryRhs, original Query and onLeftKey
     * @param subQueryRhs
     * @param originalQuery
     * @param onLeftKey
     */
    JoinWhere(const Query& subQueryRhs, Query& originalQuery, const ExpressionItem& onLeftKey);

    /**
     * @brief sets the rightKey item
     * @param onRightKey
     * @return object of type JoinCondition on which windowing & the original joinWith function can be called.
     */
    [[nodiscard]] JoinCondition equalsTo(const ExpressionItem& onRightKey) const;

  private:
    const Query& subQueryRhs;
    Query& originalQuery;
    ExpressionItem onLeftKey;
};

class JoinCondition {
  public:
    /**
    * @brief: Constructor. Initialises always subQueryRhs, originalQuery, onLeftKey and onRightKey
    * @param subQueryRhs
    * @param originalQuery
    * @param onLeftKey
    * @param onRightKey
    */
    JoinCondition(const Query& subQueryRhs,
                  Query& originalQuery,
                  const ExpressionItem& onLeftKey,
                  const ExpressionItem& onRightKey);

    /**
     * @brief: calls internal the original joinWith function with all the gathered parameters.
     * @param windowType
     * @return the query with the result of the original joinWith function is returned.
     */
    [[nodiscard]] Query& window(Windowing::WindowTypePtr const& windowType) const;

  private:
    const Query& subQueryRhs;
    Query& originalQuery;
    ExpressionItem onLeftKey;
    ExpressionItem onRightKey;
};

}//namespace JoinOperatorBuilder

/**
 * User interface to create stream processing queries.
 * The current api exposes method to create queries using all currently supported operators.
 */
class Query {
  public:
    Query(const Query&);

    virtual ~Query() = default;

    friend class JoinOperatorBuilder::JoinCondition;// we need that because we make the original joinWith() private
    friend class WindowOperatorBuilder::WindowedQuery;
    friend class WindowOperatorBuilder::KeyedWindowedQuery;

    WindowOperatorBuilder::WindowedQuery window(Windowing::WindowTypePtr const& windowType);

    /**
     * @brief can be called on the original query with the query to be joined with and sets this query in the class Join.
     * @param subQueryRhs
     * @return object where where() function is defined and can be called by user
     */
    JoinOperatorBuilder::Join joinWith(const Query& subQueryRhs);

    /**
     * @brief: Creates a query from a particular source stream. The source stream is identified by its name.
     * During query processing the underlying source descriptor is retrieved from the stream catalog.
     * @param sourceStreamName name of the stream to query. This name has to be registered in the query catalog.
     * @return the query
     */
    static Query from(std::string const& sourceStreamName);

    /**
    * This looks ugly, but we can't reference to QueryPtr at this line.
    * @param subQuery is the query to be unioned
    * @return the query
    */
    Query& unionWith(Query* subQuery);

    /**
     * @brief this call projects out the attributes in the parameter list
     * @param attribute list
     * @return the query
     */
    template<typename... Args>
    auto project(Args&&... args) -> std::enable_if_t<std::conjunction_v<std::is_constructible<ExpressionItem, Args>...>, Query&> {
        std::vector const expressions{ExpressionItem{std::forward<Args>(args)}.getExpressionNode()...};
        OperatorNodePtr op = LogicalOperatorFactory::createProjectionOperator(expressions);
        queryPlan->appendOperatorAsNewRoot(op);
        return *this;
    }

    /**
     * This looks ugly, but we can't reference to QueryPtr at this line.
     * @param new stream name
     * @return the query
     */
    Query& as(std::string const& newStreamName);

    /**
     * @brief: Filter records according to the predicate.
     * @example filter(Attribute("f1" < 10))
     * @param predicate as expression node
     * @return the query
     */
    Query& filter(ExpressionNodePtr const& filterExpression);

    /**
     * @brief: Create watermark assginer operator.
     * @param onField filed to retrieve the timestamp for watermark.
     * @param delay timestamp delay of the watermark.
     * @return query.
     */
    Query& assignWatermark(Windowing::WatermarkStrategyDescriptorPtr const& watermarkStrategyDescriptor);

    /**
     * @brief: Map records according to a map expression.
     * @example map(Attribute("f2") = Attribute("f1") * 42 )
     * @param map expression
     * @return query
     */
    Query& map(FieldAssignmentExpressionNodePtr const& mapExpression);

    /**
     * @brief Add sink operator for the query.
     * The Sink operator is defined by the sink descriptor, which represents the semantic of this sink.
     * @param sinkDescriptor
     */
    virtual Query& sink(SinkDescriptorPtr sinkDescriptor);

    /**
     * @brief Gets the query plan from the current query.
     * @return QueryPlan
     */
    QueryPlanPtr getQueryPlan();

    // creates a new query object
    Query(QueryPlanPtr queryPlan);

  protected:
    // query plan containing the operators.
    QueryPlanPtr queryPlan;

  private:
    /**
     * @new change: Now it's private, because we don't want the user to have access to it.
     * We call it only internal as a last step during the Join operation
     * @brief This methods add the join operator to a query
     * @param subQueryRhs subQuery to be joined
     * @param onLeftKey key attribute of the left stream
     * @param onLeftKey key attribute of the right stream
     * @param windowType Window definition.
     * @return the query
     */
    Query& joinWith(const Query& subQueryRhs,
                    ExpressionItem onLeftKey,
                    ExpressionItem onRightKey,
                    Windowing::WindowTypePtr const& windowType);

    /**
     * @new change: similar to join, the original window and windowByKey become private --> only internal use
     * @brief: Creates a window aggregation.
     * @param windowType Window definition.
     * @param aggregation Window aggregation function.
     * @return query.
     */
    Query& window(Windowing::WindowTypePtr const& windowType, Windowing::WindowAggregationPtr const& aggregation);

    /**
      * @brief: Creates a window aggregation.
      * @param windowType Window definition.
      * @param aggregation Window aggregation function.
      * @return query.
      */
    Query& windowByKey(ExpressionItem onKey,
                       Windowing::WindowTypePtr const& windowType,
                       Windowing::WindowAggregationPtr const& aggregation);
};

using QueryPtr = std::shared_ptr<Query>;

}// namespace NES

#endif

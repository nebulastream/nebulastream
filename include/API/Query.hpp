#ifndef API_QUERY_H
#define API_QUERY_H

#include <API/Expressions/ArithmeticalExpressions.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <API/Schema.hpp>
#include <API/Windowing.hpp>

#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Sources/DataSource.hpp>
#ifdef ENABLE_KAFKA_BUILD
#include <cppkafka/configuration.h>
#endif// KAFKASINK_HPP
#include <string>

namespace NES {

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class ExpressionNode;
typedef std::shared_ptr<ExpressionNode> ExpressionNodePtr;

class SourceLogicalOperatorNode;
typedef std::shared_ptr<SourceLogicalOperatorNode> SourceLogicalOperatorNodePtr;

class SinkLogicalOperatorNode;
typedef std::shared_ptr<SinkLogicalOperatorNode> SinkLogicalOperatorNodePtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class WindowType;
typedef std::shared_ptr<WindowType> WindowTypePtr;

class WindowAggregationDescriptor;
typedef std::shared_ptr<WindowAggregationDescriptor> WindowAggregationPtr;

using namespace NES::API;
using namespace NES::Windowing;

/**
 * User interface to create stream processing queries.
 * The current api exposes method to create queries using all currently supported operators.
 */
class Query {
  public:
    Query(const Query&);

    ~Query() = default;

    /**
     * @brief: Creates a query from a particular source stream. The source stream is identified by its name.
     * During query processing the underlying source descriptor is retrieved from the stream catalog.
     * @param sourceStreamName name of the stream to query. This name has to be registered in the query catalog.
     * @return the query
     */
    static Query from(const std::string sourceStreamName);

    /**
     * This looks ugly, but we can't reference to QueryPtr at this line.
     * @param subQuery is the query to be merged
     * @return
     */
    Query& merge(Query* subQuery);

    /**
     * @brief Create Query using queryPlan
     * @param sourceStreamName source stream name
     * @param queryPlan the input query plan
     * @return Query instance
     */
    static Query createFromQueryPlan(QueryPlanPtr queryPlan);

    /**
     * @brief: Filter records according to the predicate.
     * @example filter(Attribute("f1" < 10))
     * @param predicate as expression node
     * @return the query
     */
    Query& filter(const ExpressionNodePtr filterExpression);

    /**
     * @brief: Creates a window aggregation.
     * @param windowType Window definition.
     * @param aggregation Window aggregation function.
     * @return query.
     */
    Query& windowByKey(const ExpressionItem onKey,
                       const Windowing::WindowTypePtr windowType,
                       const Windowing::WindowAggregationPtr aggregation);


    Query& assignWatermark(ExpressionItem onField, TimeMeasure delay);

    /**
     * @brief: Creates a window aggregation.
     * @param windowType Window definition.
     * @param aggregation Window aggregation function.
     * @return query.
     */
    Query& window(const Windowing::WindowTypePtr windowType,
                  const Windowing::WindowAggregationPtr aggregation);

    /**
     * @brief: Map records according to a map expression.
     * @example map(Attribute("f2") = Attribute("f1") * 42 )
     * @param map expression
     * @return query
     */
    Query& map(const FieldAssignmentExpressionNodePtr mapExpression);

    /**
     * @brief Add sink operator for the query.
     * The Sink operator is defined by the sink descriptor, which represents the semantic of this sink.
     * @param sinkDescriptor
     */
    virtual Query& sink(const SinkDescriptorPtr sinkDescriptor);

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
};

typedef std::shared_ptr<Query> QueryPtr;

}// namespace NES

#endif

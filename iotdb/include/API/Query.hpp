#ifndef API_QUERY_H
#define API_QUERY_H

#include <API/Expressions/ArithmeticalExpressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Config.hpp>
#include <API/Schema.hpp>
#include <API/Stream.hpp>

#include <string>
#include <SourceSink/DataSource.hpp>
#include <cppkafka/configuration.h>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

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

/**
 * User interface to create stream processing queries.
 * The current api exposes method to create queries using all currently supported operators.
 */
class Query {
  public:
    ~Query() = default;

    /**
     * @brief: Creates a query from a particular source stream. The source stream is identified by its name.
     * During query processing the underlying source descriptor is retrieved from the stream catalog.
     * @param sourceStreamName name of the stream to query. This name has to be registered in the query catalog.
     * @return the query
     */
    static Query from(const std::string sourceStreamName);

    /**
     * @brief Create Query using queryPlan
     * @param sourceStreamName source stream name
     * @param queryPlan the input query plan
     * @return Query instance
     */
    static Query createFromQueryPlan(std::string sourceStreamName, QueryPlanPtr queryPlan);

    /**
     * @brief: Filter records according to the predicate.
     * filter(Attribute("f1" < 10))
     * @param predicate as expression node
     * @return the query
     */
    Query& filter(const ExpressionNodePtr filterExpression);

    /**
     * @brief: Map records according to a map expression.
     * map(Attribute("f2") = Attribute("f1") * 42 )
     * @param map expression
     * @return query
     */
    Query& map(const FieldAssignmentExpressionNodePtr mapExpression);

    /**
     * @brief Add sink operator for the query.
     * The Sink operator is defined by the sink descriptor, which represents the semantic of this sink.
     * @param sinkDescriptor
     */
    Query& sink(const SinkDescriptorPtr sinkDescriptor);

    /**
     * @brief Gets the query plan from the current query.
     * @return QueryPlan
     */
    QueryPlanPtr getQueryPlan();

    /**
     * @brief Get the source stream name
     * @return sourceStreamName
     */
    const std::string getSourceStreamName() const;

  private:
    // creates a new query object
    Query(std::string sourceStreamName, QueryPlanPtr queryPlan);
    // query plan containing the operators.
    QueryPlanPtr queryPlan;
    std::string sourceStreamName;
};

typedef std::shared_ptr<Query> QueryPtr;

}// namespace NES

#endif

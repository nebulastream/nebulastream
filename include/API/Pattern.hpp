#ifndef API_PATTERN_H
#define API_PATTERN_H

#include <API/Config.hpp>
#include <API/Expressions/ArithmeticalExpressions.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <API/Schema.hpp>
#include <API/Stream.hpp>
#include <API/Query.hpp>

#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <SourceSink/DataSource.hpp>
#include <cppkafka/configuration.h>
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

/**
 * User interface to specify a pattern query.
 */
class Pattern : public Query {

  public:
    Pattern(const Pattern&);
    ~Pattern() = default;

    /**
     * @brief: Monitors a condition from a particular source stream. The source stream is identified by its name.
     * During query processing the underlying source descriptor is retrieved from the stream catalog.
     * @param sourceStreamName name of the stream to monitor. This name has to be registered in the query catalog.
     * //TODO own catalog for patterns?
     * @return the pattern
     */
    static Pattern from(const std::string sourceStreamName);

    /**
     * @brief Create Query using queryPlan
     * @param sourceStreamName source stream name
     * @param queryPlan the input query plan
     * @return Query instance
     */
    static Pattern createFromQueryPlan(QueryPlanPtr queryPlan);

    /**
     * @brief: Filter records according to the predicate.
     * filter(Attribute("f1" < 10))
     * @param predicate as expression node
     * @return the query
     */
    Pattern& filter(const ExpressionNodePtr filterExpression);

    /**
     * @brief: Map records according to a map expression.
     * map(Attribute("f2") = Attribute("f1") * 42 )
     * @param map expression
     * @return query
     */
    Pattern& map(const FieldAssignmentExpressionNodePtr mapExpression);

    /**
     * @brief Add sink operator for the query.
     * The Sink operator is defined by the sink descriptor, which represents the semantic of this sink.
     * @param sinkDescriptor
     */
    Pattern& sink(const SinkDescriptorPtr sinkDescriptor);

    /**
     * @brief Gets the query plan from the current query.
     * @return QueryPlan
     */
    QueryPlanPtr getQueryPlan();
  private:
    // creates a new query object
    Pattern(QueryPlanPtr queryPlan);
    // query plan containing the operators.
    QueryPlanPtr queryPlan;
};

typedef std::shared_ptr<Pattern> PatternPtr;

}// namespace NES

#endif

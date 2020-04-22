#ifndef API_INPUT_QUERY_H
#define API_INPUT_QUERY_H

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
 * Interface to create new query.
 */
class Query {
  public:

    ~Query() = default;

    /**
     * @brief: Creates a query from a particualr stream.
     * @param sourceStream name of the stream to query.
     * @param schema @deprecated will be removed when we have the catalog.
     * @return the input query
     */
    static Query from(Stream& stream);

    /**
     * @brief: Filter records according to the predicate.
     * @param predicate
     * @return query
     */
    Query& filter(const ExpressionNodePtr filterExpression);

    /**
     * @brief: Map records to the resultField by the predicate.
     * @param resultField
     * @param predicate
     * @return query
     *
     * @Caution : The method is not implemented yet.
     */
    Query& map(const FieldAssignmentExpressionNodePtr mapExpression);

    /**
     * @brief: Unify two queries.
     * All records are contained in the result stream
     * @return query
     *
     * @Caution : The method is not implemented yet.
     */
    Query& combine(const Query& subQuery);

    /**
     * @brief: Joins two streams according to the join predicate.
     * @param subQuery right query.
     * @param joinPred join predicate.
     * @return query.
     *
     * @Caution : The method is not implemented yet.
     */
    Query& join(const Query& subQuery,
                const JoinPredicatePtr joinPred);

    /**
     * @brief: Creates a window by a key.
     *
     * @Caution : The method is not implemented yet.
     */
    Query& windowByKey();

    /**
     * @brief: Creates a window aggregation.
     *
     * @Caution : The method is not implemented yet.
     */
    Query& window();

    /**
     * @brief: Registers the query as a source in the catalog.
     * @param name the name for the result stream.
     *
     * @Caution : The method is not implemented yet.
     */
    Query& to(const std::string& name);

    /**
     * @brief Add sink operator for the query
     * @param sinkDescriptor : pointer to the descriptor of the sink
     *
     */
    Query& sink(const SinkDescriptorPtr sinkDescriptor);

    /**
     * @brief get source stream
     * @return pointer to source stream
     */
    const StreamPtr getSourceStream() const;

    // Copy constructors
    Query(const Query&);

    /**
     * @brief Returns string representation of the query.
     */
    std::string toString();

    /**
     * @brief Gets the query plan from the input query.
     */
    QueryPlanPtr getQueryPlan();

  private:
    Query(QueryPlanPtr queryPlan);
    // query plan containing the operators.
    QueryPlanPtr queryPlan;
};

typedef std::shared_ptr<Query> QueryPtr;

}  // namespace NES

#endif

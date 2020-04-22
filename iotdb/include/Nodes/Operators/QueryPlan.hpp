#ifndef NES_INCLUDE_NODES_GRAPH_HPP_
#define NES_INCLUDE_NODES_GRAPH_HPP_
#include <memory>
#include <vector>

namespace NES {

class Stream;
typedef std::shared_ptr<Stream> StreamPtr;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class SourceLogicalOperatorNode;
typedef std::shared_ptr<SourceLogicalOperatorNode> SourceLogicalOperatorNodePtr;

class SinkLogicalOperatorNode;
typedef std::shared_ptr<SinkLogicalOperatorNode> SinkLogicalOperatorNodePtr;

/**
 * @brief The query plan encapsulates a set of operators and provides a set of utility functions.
 */
class QueryPlan {
  public:

    /**
     * @brief Creates a new query plan with a root operator and a specific stream.
     * @param rootOperator The root operator usually a source operator.
     * @param stream the source stream attached to this query.
     * @return a pointer to the query plan.
     */
    static QueryPlanPtr create(OperatorNodePtr rootOperator, StreamPtr sourceStream);

    /**
     * @brief Get all source operators
     * @return vector of logical source operators
     */
    std::vector<SourceLogicalOperatorNodePtr> getSourceOperators();

    /**
     * @brief Get all sink operators
     * @return vector of logical sink operators
     */
    std::vector<SinkLogicalOperatorNodePtr> getSinkOperators();


    /**
     * @brief Appends an operator to the query plan. This updates the operator id of the query.
     * @param op
     */
    void appendOperator(OperatorNodePtr op);

    /**
     * @brief get source stream
     * @return pointer to source stream
     */
    const StreamPtr getSourceStream() const;

    /**
     * @brief Returns string representation of the query.
     */
    std::string toString();

  private:
    QueryPlan(OperatorNodePtr rootOperator, StreamPtr stream);
    OperatorNodePtr rootOperator;
    uint64_t currentOperatorId;
    StreamPtr sourceStream;
    /**
     * @brief Get next free operator id
     */
    uint64_t getNextOperatorId();
};
}
#endif //NES_INCLUDE_NODES_GRAPH_HPP_

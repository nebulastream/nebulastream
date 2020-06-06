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

class BreadthFirstNodeIterator;
typedef std::shared_ptr<BreadthFirstNodeIterator> BreadthFirstNodeIteratorPtr;

/**
 * @brief The query plan encapsulates a set of operators and provides a set of utility functions.
 */
class QueryPlan {
  public:
    /**
     * @brief Creates a new query plan with a root operator.
     * @param rootOperator The root operator usually a source operator.
     * @return a pointer to the query plan.
     */
    static QueryPlanPtr create(std::string sourceStreamName, OperatorNodePtr rootOperator);

    /**
     * @brief Creates a new query plan without and operator.
     * @return a pointer to the query plan
     */
    static QueryPlanPtr create();

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
     * @brief Appends an operator to the query plan and make the new operator as root.
     * This updates the operator id of the query.
     * @param operatorNode : new operator
     */
    void appendOperator(OperatorNodePtr operatorNode);

    /**
     * @brief Append the new system generated operator to the query plan.
     * Note: this operation will add operator without Id.
     * @param bottom if true will append the operator at the bottom of the graph else at the top and make it as new root.
     * @param operatorNode
     * FIXME: add methods to add system generated at different locations
     */
    void appendSystemGeneratedOperator(bool bottom, OperatorNodePtr operatorNode);

    /**
     * @brief Returns string representation of the query.
     */
    std::string toString();

    /**
     * @brief Get the root operator of the query graph.
     * NOTE: root operator of a query plan is usually a sink operator
     * FIXME: We might have multiple root nodes or we might need a dummy root node otherwise.
     * @return
     */
    OperatorNodePtr getRootOperator() const;

    /**
     * @brief Get the source stream name
     * @return sourceStreamName
     */
    const std::string getSourceStreamName() const;
    /**
     * Find if the operator exists in the plan
     * @param operatorNode
     * @return true if the operator exists else false
     */
    bool hasOperator(OperatorNodePtr operatorNode);

    /**
     * Set the query Id for the plan
     * @param queryId
     */
    void setQueryId(std::string& queryId);

    /**
     * Get the queryId for the plan
     * @return
     */
    const std::string& getQueryId() const;

  private:
    /**
     * @brief initialize query plan and set currentOperatorId to 1
     * @param rootOperator
     */
    explicit QueryPlan(std::string sourceStreamName, OperatorNodePtr rootOperator);

    explicit QueryPlan();

    /**
     * @brief Get next free operator id
     */
    uint64_t getNextOperatorId();

    BreadthFirstNodeIteratorPtr bfsIterator;
    OperatorNodePtr rootOperator;
    uint64_t currentOperatorId;
    std::string queryId;
    std::string sourceStreamName;
};
}// namespace NES
#endif//NES_INCLUDE_NODES_GRAPH_HPP_

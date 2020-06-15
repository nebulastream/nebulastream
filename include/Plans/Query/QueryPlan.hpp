#ifndef NES_INCLUDE_PLANS_QUERY_HPP_
#define NES_INCLUDE_PLANS_QUERY_HPP_
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
     * @brief Append the new pre-existing operator to the query plan.
     * Note: Pre-existing operators are the operators that are already part of another
     * query plan and are getting moved to another query plan or are the system created network sink or source operators
     * Note: this operation will add the pre-existing operator without assigning it a new Id.
     * @param operatorNode
     */
    void appendPreExistingOperator(OperatorNodePtr operatorNode);

    /**
     * @brief Pre-pend the pre-existing operator to the leaf of the query plan.
     * Note: Pre-existing operators are the operators that are already part of another
     * query plan and are getting moved to another query plan or are the system created network sink or source operators
     * Note: this operation will add the pre-existing operator without assigning it a new Id.
     * @param operatorNode
     */
    void prependPreExistingOperator(OperatorNodePtr operatorNode);

    /**
     * @brief Returns string representation of the query.
     */
    std::string toString();

    /**
     * @brief Get the list of root operators for the query graph.
     * NOTE: in certain stages the sink operators might not be the root operators
     * @return
     */
    std::vector<OperatorNodePtr> getRootOperators();

    /**
     * @brief Get all the leaf operators in the query plan
     * Note: in certain stages the source operators might not be Leaf operators
     * @return returns a vector of leaf operators
     */
    std::vector<OperatorNodePtr> getLeafOperators();

    /**
     * @brief Get the source stream name
     * @return sourceStreamName
     */
    const std::string getSourceStreamName() const;

    /**
     * Find if the operator with same Id exists in the plan.
     * Note: This method only check if there exists another operator with same Id or not.
     * Note: The system generated operators are ignored from this check.
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
     * @return query Id of the plan
     */
    const std::string& getQueryId() const;

  private:
    /**
     * @brief initialize query plan and set currentOperatorId to 1
     * @param rootOperator
     */
    explicit QueryPlan(std::string sourceStreamName, OperatorNodePtr rootOperator);

    /**
     * @brief initialize an empty query plan
     */
    explicit QueryPlan();

    /**
     * @brief Get next free operator id
     */
    uint64_t getNextOperatorId();

    std::vector<OperatorNodePtr> rootOperators;
    uint64_t currentOperatorId;
    std::string queryId;
    std::string sourceStreamName;
};
}// namespace NES
#endif//NES_INCLUDE_PLANS_QUERY_HPP_

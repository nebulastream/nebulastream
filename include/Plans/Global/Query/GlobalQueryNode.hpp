#ifndef NES_GLOBALQUERYNODE_HPP
#define NES_GLOBALQUERYNODE_HPP

#include <Nodes/Node.hpp>
#include <memory>
#include <string>
#include <vector>

namespace NES {

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class GlobalQueryNode;
typedef std::shared_ptr<GlobalQueryNode> GlobalQueryNodePtr;

/**
 * @brief This class encapsulates the logical operators belonging to a set of queries.
 */
class GlobalQueryNode : public Node {

  public:
    /**
     * @brief Creates empty global query node
     */
    static GlobalQueryNodePtr createEmpty();

    /**
     * @brief Global Query Operator builder
     * @param queryId: query id of the query
     * @param operatorNode: logical operator
     * @return Shared pointer to the instance of Global Query Operator instance
     */
    static GlobalQueryNodePtr create(std::string queryId, OperatorNodePtr operatorNode);

    /**
     * @brief Add the id of a new query to the Global Query Operator
     * @param queryId : query id
     */
    void addQuery(std::string queryId);

    /**
     * @brief Add the id of the new query tot he Global Query Operator and
     * @param queryId
     * @param operatorNode
     */
    void addQueryAndOperator(std::string queryId, OperatorNodePtr operatorNode);

    /**
     * @brief add a new query Id and a new logical operator
     * @param queryId : query to be added.
     * @param operatorNode : logical operator to be grouped together.
     */
    void addQuery(std::string queryId, OperatorNodePtr operatorNode);

    /**
     * @brief Remove the query
     * @param queryId
     */
    void removeQuery(std::string queryId);

    /**
     * @brief Check if the global query node was updated.
     * @return true if updated else false.
     */
    bool wasUpdated();

  private:
    GlobalQueryNode(std::string queryId, OperatorNodePtr operatorNode);
    GlobalQueryNode();
    std::vector<std::string> queryIds;
    std::vector<OperatorNodePtr> logicalOperators;
    std::map<std::string, OperatorNodePtr> queryToOperatorMap;
    bool scheduled;
    bool operatorListUpdated;
};
}// namespace NES
#endif//NES_GLOBALQUERYNODE_HPP

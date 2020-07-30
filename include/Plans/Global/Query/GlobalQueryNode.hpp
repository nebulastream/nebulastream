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
     * @param id: id of the global query node
     */
    static GlobalQueryNodePtr createEmpty(uint64_t id);

    /**
     * @brief Global Query Operator builder
     * @param id: id of the global query node
     * @param queryId: query id of the query
     * @param operatorNode: logical operator
     * @return Shared pointer to the instance of Global Query Operator instance
     */
    static GlobalQueryNodePtr create(uint64_t id, std::string queryId, OperatorNodePtr operatorNode);

    /**
     * @brief Get id of the node
     * @return node id
     */
    uint64_t getId();

    /**
     * @brief Add the id of a new query to the Global Query Operator
     * @param queryId : query id
     */
    void addQuery(std::string queryId);

    /**
     * @brief add a new query Id and a new logical operator
     * @param queryId : query to be added.
     * @param operatorNode : logical operator to be grouped together.
     */
    void addQueryAndOperator(std::string queryId, OperatorNodePtr operatorNode);

    /**
     * @brief Remove the query
     * @param queryId
     * @return true if successful
     */
    bool removeQuery(const std::string& queryId);

    /**
     * @brief Check if the global query node was updated.
     * @return true if updated else false.
     */
    bool hasNewUpdate();

    /**
     * @brief Check if logical operator already present in the node
     * @param operatorNode : logical operator to check
     * @return true if logical operator already present.
     */
    bool hasOperator(OperatorNodePtr operatorNode);

    /**
     * @brief Check if the Global query node is empty or not.
     * @return true if there exists no logical query operator in the node.
     */
    bool isEmpty();

    /**
     * @brief Mark the GlobalQueryNode as updated
     */
    void markAsUpdated();

    /**
     * @brief helper function of get global query nodes with specific logical operator type
     */
    template<class T>
    void getNodesWithTypeHelper(std::vector<GlobalQueryNodePtr>& foundNodes) {

        if (logicalOperators.empty()) {
            return;
        }

        for (auto logicalOperator : logicalOperators) {
            if (logicalOperators[0]->instanceOf<T>()) {
                foundNodes.push_back(shared_from_this()->as<GlobalQueryNode>());
                break;
            }
        }

        for (auto& successor : this->children) {
            successor->as<GlobalQueryNode>()->getNodesWithTypeHelper<T>(foundNodes);
        }
    }

    const std::string toString() const override;

  private:
    GlobalQueryNode(uint64_t id);
    GlobalQueryNode(uint64_t id, std::string queryId, OperatorNodePtr operatorNode);
    uint64_t id;
    std::vector<std::string> queryIds;
    std::vector<OperatorNodePtr> logicalOperators;
    std::map<std::string, OperatorNodePtr> queryToOperatorMap;
    std::map<OperatorNodePtr, std::vector<std::string>> operatorToQueryMap;
    bool scheduled;
    bool querySetUpdated;
    bool operatorSetUpdated;
};
}// namespace NES
#endif//NES_GLOBALQUERYNODE_HPP

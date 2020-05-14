#ifndef EXECUTIONNODE_HPP
#define EXECUTIONNODE_HPP

#include <memory>
#include <vector>

namespace NES {

class NESTopologyEntry;
typedef std::shared_ptr<NESTopologyEntry> NESTopologyEntryPtr;

class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

/**\brief:
 *          Contains information about the list of operators and the node where the operators are to be executed.
 */
class ExecutionNode {

  public:

    ExecutionNode(std::string operatorName, std::string nodeName, NESTopologyEntryPtr nesNode, OperatorPtr rootOperator);

    size_t getId() { return this->id; };

    std::string getOperatorName() {
        return this->operatorName;
    }

    std::string getNodeName() {
        return this->nodeName;
    }

    void setOperatorName(const std::string& operatorName) {
        this->operatorName = operatorName;
    }

    NESTopologyEntryPtr getNESNode() {
        return nesNode;
    }

    void setRootOperator(const OperatorPtr root) {
        this->rootOperator = root;
    }

    OperatorPtr getRootOperator() {
        return rootOperator;
    }

    std::vector<size_t>& getChildOperatorIds() {
        return operatorIds;
    }

    /**
     * @brief Add the input operator as the parent of the last operator in the chain
     * Note: root is always the child operator.
     * @param operatorPtr  : operator to be added
     */
    void addOperator(OperatorPtr operatorPtr);

    /**
     * @brief Add the input operator as the child of the last operator in the chain
     * Note: root is always the child operator.
     * @param operatorPtr  : operator to be added
     */
    void addChild(OperatorPtr operatorPtr) ;

    void addOperatorId(size_t childOperatorId) {
        this->operatorIds.push_back(childOperatorId);
    }

  private:
    uint64_t id;
    std::string operatorName;
    std::string nodeName;
    OperatorPtr rootOperator;
    NESTopologyEntryPtr nesNode;
    std::vector<size_t> operatorIds;
};

typedef std::shared_ptr<ExecutionNode> ExecutionNodePtr;
}

#endif //EXECUTIONNODE_HPP

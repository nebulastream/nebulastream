/**\brief:
 *          Contains information about the list of operators and the node where the operators are to be executed.
 */

#ifndef EXECUTIONNODE_HPP
#define EXECUTIONNODE_HPP

#include <Operators/Operator.hpp>
#include <Topology/NESTopologyEntry.hpp>

using namespace std;

namespace NES {

class ExecutionNode {

  public:
    ExecutionNode(std::string operatorName, std::string nodeName, NESTopologyEntryPtr nesNode,
                  OperatorPtr rootOperator) {
        this->id = nesNode->getId();
        this->operatorName = operatorName;
        this->nodeName = nodeName;
        this->nesNode = nesNode;
        this->rootOperator = rootOperator;
    };

    int getId() { return this->id; };

    std::string getOperatorName() {
        return this->operatorName;
    }

    std::string getNodeName() {
        return this->nodeName;
    }

    void setOperatorName(const string& operatorName) {
        this->operatorName = operatorName;
    }

    NESTopologyEntryPtr& getNESNode() {
        return nesNode;
    }

    void setRootOperator(const OperatorPtr root) {
        this->rootOperator = root;
    }

    OperatorPtr& getRootOperator() {
        return rootOperator;
    }

    vector<size_t>& getChildOperatorIds() {
        return operatorIds;
    }

    /**
   * @brief Add the input operator as the parent of the last operator in the chain
   * Note: root is always the child operator.
   * @param operatorPtr  : operator to be added
   */
    void addOperator(OperatorPtr operatorPtr) {
        OperatorPtr root = rootOperator;
        while (root->getParent() != nullptr) {
            root = root->getParent();
        }
        operatorPtr->setChildren({root});
        root->setParent(operatorPtr);
    }

    /**
     * @brief Add the input operator as the child of the last operator in the chain
     * Note: root is always the child operator.
     * @param operatorPtr  : operator to be added
     */
    void addChild(OperatorPtr operatorPtr) {
        OperatorPtr root = rootOperator;
        root->setChildren({operatorPtr});
        operatorPtr->setParent(root);
        rootOperator = operatorPtr;
    }

    void addOperatorId(size_t childOperatorId) {
        this->operatorIds.push_back(childOperatorId);
    }

  private:
    int id;
    string operatorName;
    string nodeName;
    OperatorPtr rootOperator;
    NESTopologyEntryPtr nesNode;
    vector<size_t> operatorIds{};
};

typedef std::shared_ptr<ExecutionNode> ExecutionNodePtr;
}// namespace NES

#endif//EXECUTIONNODE_HPP

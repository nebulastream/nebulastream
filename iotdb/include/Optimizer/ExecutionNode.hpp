/**\brief:
 *          Contains information about the list of operators and the node where the operators are to be executed.
 */

#ifndef EXECUTIONNODE_HPP
#define EXECUTIONNODE_HPP

#include "../Topology/NESTopologyEntry.hpp"

namespace NES {

using namespace std;

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

  void setOperatorName(const string &operatorName) {
    this->operatorName = operatorName;
  }

  NESTopologyEntryPtr &getNESNode() {
    return nesNode;
  }

  void setRootOperator(const OperatorPtr root) {
    this->rootOperator = root;
  }

  OperatorPtr &getRootOperator() {
    return rootOperator;
  }

  vector<int> &getChildOperatorIds() {
    return childOperatorIds;
  }

  void addChildOperatorId(int childOperatorId) {
    this->childOperatorIds.push_back(childOperatorId);
  }

 private:
  int id;
  string operatorName;
  string nodeName;
  OperatorPtr rootOperator;
  NESTopologyEntryPtr nesNode;
  vector<int> childOperatorIds{};
};

typedef std::shared_ptr<ExecutionNode> ExecutionNodePtr;
}

#endif //EXECUTIONNODE_HPP

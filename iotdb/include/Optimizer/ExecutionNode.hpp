/**\brief:
 *          Contains information about the list of operators and the node where the operators are to be executed.
 */

#ifndef IOTDB_EXECUTIONNODE_HPP
#define IOTDB_EXECUTIONNODE_HPP

#include <Topology/FogTopologyEntry.hpp>

namespace iotdb {

using namespace std;

class ExecutionNode {

 public:
  ExecutionNode(std::string operatorName, std::string nodeName, FogTopologyEntryPtr fogNode,
                OperatorPtr rootOperator) {
    this->id = fogNode->getId();
    this->operatorName = operatorName;
    this->nodeName = nodeName;
    this->fogNode = fogNode;
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

  FogTopologyEntryPtr &getFogNode() {
    return fogNode;
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
  FogTopologyEntryPtr fogNode;
  vector<int> childOperatorIds{};
};

typedef std::shared_ptr<ExecutionNode> ExecutionNodePtr;
}

#endif //IOTDB_EXECUTIONNODE_HPP

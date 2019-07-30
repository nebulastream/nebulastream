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
                      OperatorPtr executableOperator) {
            this->id = fogNode->getId();
            this->operatorName = operatorName;
            this->nodeName = nodeName;
            this->fogNode = fogNode;
            vector<OperatorPtr> operators{executableOperator};
            this->executableOperators = operators;
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

        void setFogNode(FogTopologyEntryPtr &fogNode) {
            this->fogNode = fogNode;
        }

        vector<OperatorPtr> &getExecutableOperator() {
            return executableOperators;
        }

        void addExecutableOperator(OperatorPtr &executableOperator) {
            this->executableOperators.push_back(executableOperator);
        }

    private:
        int id;
        string operatorName;
        string nodeName;
        FogTopologyEntryPtr fogNode;
        vector<OperatorPtr> executableOperators;
    };


    typedef std::shared_ptr<ExecutionNode> ExecutionNodePtr;
}

#endif //IOTDB_EXECUTIONNODE_HPP

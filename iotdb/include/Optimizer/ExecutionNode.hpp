/**\brief:
 *          Contains information about the operator and the node where the operator is to be executed.
 *
 * Assumption : We will have only one operator and one node at a given point of time. In case of parallelization, we
 * will have collection of this object.
 *
 */

#ifndef IOTDB_EXECUTIONNODE_HPP
#define IOTDB_EXECUTIONNODE_HPP


#include <Topology/FogTopologyEntry.hpp>

namespace iotdb {
    static size_t currentNodeId = 1;

    class ExecutionNode {

    public:
        ExecutionNode(std::string operatorName, std::string nodeName, FogTopologyEntryPtr nodeAndOperator) {
            this->id = currentNodeId++;
            this->operatorName = operatorName;
            this->nodeName = nodeName;
            this->nodeAndOperator = nodeAndOperator;
        };

        int getId() { return this->id; };

        std::string getOperatorName() {
            return this->operatorName;
        }

        std::string getNodeName() {
            return this->nodeName;
        }

        FogTopologyEntryPtr getNoeAndOperator() {
            return this->nodeAndOperator;
        }

    private:
        int id;
        std::string operatorName;
        std::string nodeName;
        FogTopologyEntryPtr nodeAndOperator;
    };


    typedef std::shared_ptr <ExecutionNode> ExecutionNodePtr;
}

#endif //IOTDB_EXECUTIONNODE_HPP

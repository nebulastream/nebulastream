/**\brief:
 *          Contains information about the link between two operator and execution node.
 *
 * Assumption : We will have only one link per execution node. In case of parallelization, we
 * will have collection of this object.
 *
 */

#ifndef IOTDB_EXECUTIONNODELINK_HPP
#define IOTDB_EXECUTIONNODELINK_HPP

#include "ExecutionNode.hpp"


namespace iotdb {
    static int currentLinkID = 1;

    class ExecutionNodeLink {

    public:

        ExecutionNodeLink(ExecutionNodePtr src, ExecutionNodePtr dest) {
            linkId = currentLinkID++;
            this->src = src;
            this->dest = dest;
        }

        int getLinkId() { return this->linkId;}
        ExecutionNodePtr getSource() { return this->src;}
        ExecutionNodePtr getDestination() { return this->dest;}

    private:

        int linkId;
        ExecutionNodePtr src;
        ExecutionNodePtr dest;
    };

    typedef std::shared_ptr <ExecutionNodeLink> ExecutionNodeLinkPtr;
}

#endif //IOTDB_EXECUTIONNODELINK_HPP

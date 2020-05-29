/**\brief:
 *          Contains information about the link between two operator and execution node.
 *
 * Assumption : We will have only one link per execution node. In case of parallelization, we
 * will have collection of this object.
 *
 */

#ifndef EXECUTIONNODELINK_HPP
#define EXECUTIONNODELINK_HPP

#include "ExecutionNode.hpp"

namespace NES {

class ExecutionNodeLink {

  public:
    ExecutionNodeLink(size_t linkId, ExecutionNodePtr src, ExecutionNodePtr dest, size_t linkCapacity, size_t linkLatency) {
        this->linkId = linkId;
        this->src = src;
        this->dest = dest;
        this->linkCapacity = linkCapacity;
        this->linkLatency = linkLatency;
    }

    size_t getLinkId() { return this->linkId; }
    ExecutionNodePtr getSource() { return this->src; }
    ExecutionNodePtr getDestination() { return this->dest; }
    size_t getLinkLatency() { return linkLatency; }
    size_t getLinkCapacity() { return linkCapacity; }

  private:
    /**
     * @brief unique link identifier.
     */
    size_t linkId;

    /**
     * @brief source execution node pointer
     */
    ExecutionNodePtr src;

    /**
     * @brief destination execution node pointer
     */
    ExecutionNodePtr dest;

    /**
     * @brief latency of the link
     */
    size_t linkLatency;

    /**
     * @brief capacity of the link
     */
    size_t linkCapacity;
};

typedef std::shared_ptr<ExecutionNodeLink> ExecutionNodeLinkPtr;
}// namespace NES

#endif//EXECUTIONNODELINK_HPP

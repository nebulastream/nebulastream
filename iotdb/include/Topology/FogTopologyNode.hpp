#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYNODE_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYNODE_HPP_

#include <memory>
#include <vector>
#include "FogTopologyEntry.hpp"
#define INVALID_NODE_ID 101

class FogTopologyNode : public FogTopologyEntry {

public:
  FogTopologyNode() { node_id = INVALID_NODE_ID; }

  void setNodeId(size_t id) { node_id = id; }

  size_t getID() { return node_id; }

  FogNodeType getEntryType(){return Worker;}


private:
  size_t node_id;
};

typedef std::shared_ptr<FogTopologyNode> FogTopologyNodePtr;

#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYNODE_HPP_ */

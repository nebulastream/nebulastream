#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYWORKERNODE_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYWORKERNODE_HPP_

#include <memory>
#include <vector>
#include "FogTopologyEntry.hpp"
#define INVALID_NODE_ID 101

class FogTopologyWorkerNode : public FogTopologyEntry {

public:
  FogTopologyWorkerNode() { node_id = INVALID_NODE_ID; }

  void setId(size_t id) { node_id = id; }
  size_t getId() { return node_id; }

  FogNodeType getEntryType(){return Worker;}
  std::string getEntryTypeString() {return "Worker";}

private:
  size_t node_id;
};

typedef std::shared_ptr<FogTopologyWorkerNode> FogTopologyWorkerNodePtr;

#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYWORKERNODE_HPP_ */

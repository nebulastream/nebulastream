#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYWORKERNODE_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYWORKERNODE_HPP_

#include "FogTopologyEntry.hpp"
#include <API/InputQuery.hpp>

#include <memory>
#include <vector>
namespace iotdb {
#define INVALID_NODE_ID 101

class FogTopologyWorkerNode : public FogTopologyEntry {

  public:
    FogTopologyWorkerNode() { node_id = INVALID_NODE_ID; }

    void setId(size_t id) { node_id = id; }
    size_t getId() { return node_id; }

    FogNodeType getEntryType() { return Worker; }
    std::string getEntryTypeString() { return "Worker"; }

    void setQuery(InputQueryPtr pQuery) { query = pQuery; };

  private:
    size_t node_id;
    InputQueryPtr query;
};

typedef std::shared_ptr<FogTopologyWorkerNode> FogTopologyWorkerNodePtr;
} // namespace iotdb
#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYWORKERNODE_HPP_ */

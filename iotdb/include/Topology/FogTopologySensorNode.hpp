#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYSENSOR_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYSENSOR_HPP_

#include "FogTopologyEntry.hpp"
#include <memory>

#define INVALID_NODE_ID 101
namespace iotdb{

class FogTopologySensorNode : public FogTopologyEntry {

public:
  FogTopologySensorNode() { sensor_id = INVALID_NODE_ID; }

  void setId(size_t id) { sensor_id = id; }
  size_t getId() { return sensor_id; }

  FogNodeType getEntryType() { return Sensor; }
  std::string getEntryTypeString() { return "Sensor"; }

  void setQuery(InputQueryPtr pQuery){ query = pQuery;};

private:
  size_t sensor_id;
  InputQueryPtr query;
};

typedef std::shared_ptr<FogTopologySensorNode> FogTopologySensorNodePtr;
}
#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYSENSOR_HPP_ */

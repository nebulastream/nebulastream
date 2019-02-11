#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYSENSOR_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYSENSOR_HPP_

#include <memory>
#include "FogTopologyEntry.hpp"

#define INVALID_NODE_ID 101

class FogTopologySensorNode : public FogTopologyEntry{

public:
  FogTopologySensorNode() { sensorId = INVALID_NODE_ID; }

  void setId(size_t id) { sensor_id = id; }
  size_t getId() { return sensor_id; }

  FogNodeType getEntryType(){return Sensor;}
  std::string getEntryTypeString() {return "Sensor";}

private:
  size_t sensor_id;
};

typedef std::shared_ptr<FogTopologySensorNode> FogTopologySensorNodePtr;

#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYSENSOR_HPP_ */

#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYSENSOR_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYSENSOR_HPP_

#include <memory>
#include "FogTopologyEntry.hpp"

#define INVALID_NODE_ID 101

class FogTopologySensor : public FogTopologyEntry{

public:
  FogTopologySensor() { sensorID = INVALID_NODE_ID; }

  void setSensorId(size_t id) { sensorID = id; }
  size_t getID() { return sensorID; }

  FogNodeType getEntryType(){return Sensor;}

  std::string getEntryTypeString() {return "Sensor";}


private:
  size_t sensorID;
};

typedef std::shared_ptr<FogTopologySensor> FogTopologySensorPtr;

#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYSENSOR_HPP_ */

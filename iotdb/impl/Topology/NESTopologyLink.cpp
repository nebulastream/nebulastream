#include <Topology/NESTopologyLink.hpp>
#include <Util/Logger.hpp>
namespace iotdb {

LinkType NESTopologyLink::getLinkType() {
  if (sourceNode->getEntryType() == Worker
      && destNode->getEntryType() == Worker) {
    return NodeToNode;
  } else if (sourceNode->getEntryType() == Sensor
      && destNode->getEntryType() == Worker) {
    return SensorToNode;
  } else if (sourceNode->getEntryType() == Worker
      && destNode->getEntryType() == Sensor) {
    return NodeToSensor;
  }
  IOTDB_ERROR("Unrecognized LinkType!");
}

std::string NESTopologyLink::getLinkTypeString() {
  switch (getLinkType()) {
    case NodeToNode:
      return "NodeToNode";
    case SensorToNode:
      return "SensorToNode";
    case NodeToSensor:
      return "NodeToSensor";
  }
  IOTDB_ERROR("String for LinkType not found!");
}
}

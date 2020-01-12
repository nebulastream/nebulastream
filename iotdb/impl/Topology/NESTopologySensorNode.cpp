#include <Topology/NESTopologySensorNode.hpp>
namespace iotdb {

void NESTopologySensorNode::setId(size_t id) {
  this->node_id = id;
}

size_t NESTopologySensorNode::getId() {
  return node_id;
}

size_t NESTopologySensorNode::getCpuCapacity() {
  return cpuCapacity;
}

void NESTopologySensorNode::setCpuCapacity(CPUCapacity cpuCapacity) {
  this->cpuCapacity = cpuCapacity.toInt();
  this->remainingCPUCapacity = this->cpuCapacity;
}

void NESTopologySensorNode::reduceCpuCapacity(size_t usedCapacity) {
  assert(usedCapacity <= remainingCPUCapacity);
  this->remainingCPUCapacity = this->remainingCPUCapacity - usedCapacity;
}

void NESTopologySensorNode::increaseCpuCapacity(size_t freedCapacity) {
  this->remainingCPUCapacity = this->remainingCPUCapacity + freedCapacity;
}

size_t NESTopologySensorNode::getRemainingCpuCapacity() {
  return remainingCPUCapacity;
}

NESNodeType NESTopologySensorNode::getEntryType() {
  return Sensor;
}

std::string NESTopologySensorNode::getEntryTypeString() {
  return "Sensor(" + physicalStreamName + ")";
}

void NESTopologySensorNode::setQuery(InputQueryPtr pQuery) {
  this->query = pQuery;
}

std::string NESTopologySensorNode::getPhysicalStreamName() {
  return physicalStreamName;
}

void NESTopologySensorNode::setPhysicalStreamName(std::string name) {
  this->physicalStreamName = name;
}


uint16_t NESTopologySensorNode::getPublishPort() {
  return publish_port;
}

void NESTopologySensorNode::setPublishPort(uint16_t publishPort) {
  publish_port = publishPort;
}

uint16_t NESTopologySensorNode::getReceivePort() {
  return receive_port;
}

uint16_t NESTopologySensorNode::getNextFreeReceivePort() {
  receive_port++;
  return receive_port;
}

void NESTopologySensorNode::setReceivePort(uint16_t receivePort) {
  receive_port = receivePort;
}

const std::string& NESTopologySensorNode::getIp() {
  return this->ip_addr;
}

void NESTopologySensorNode::setIp(const std::string &ip) {
  this->ip_addr = ip;
}
}

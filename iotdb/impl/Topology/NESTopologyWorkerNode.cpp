#include <Topology/NESTopologyWorkerNode.hpp>

namespace NES {

size_t NESTopologyWorkerNode::getId() {
  return node_id;
}

void NESTopologyWorkerNode::setId(size_t id) {
  this->node_id = id;
}

size_t NESTopologyWorkerNode::getCpuCapacity() {
  return cpuCapacity;
}

void NESTopologyWorkerNode::setCpuCapacity(CPUCapacity cpuCapacity) {
  this->cpuCapacity = cpuCapacity.toInt();
  this->remainingCPUCapacity = this->cpuCapacity;
}

void NESTopologyWorkerNode::reduceCpuCapacity(size_t usedCapacity) {
  assert(usedCapacity <= remainingCPUCapacity);
  this->remainingCPUCapacity = this->remainingCPUCapacity - usedCapacity;
}

void NESTopologyWorkerNode::increaseCpuCapacity(size_t freedCapacity) {
  this->remainingCPUCapacity = this->remainingCPUCapacity + freedCapacity;
}

size_t NESTopologyWorkerNode::getRemainingCpuCapacity() {
  return remainingCPUCapacity;
}

bool NESTopologyWorkerNode::getIsASinkNode() {
  return this->isASink;
}

NESNodeType NESTopologyWorkerNode::getEntryType() {
  return Worker;
}

std::string NESTopologyWorkerNode::getEntryTypeString() {
  if (isASink) {
    return "sink";
  }
  return "Worker";
}

void NESTopologyWorkerNode::setQuery(InputQueryPtr pQuery) {
  this->query = pQuery;
}
;

uint16_t NESTopologyWorkerNode::getPublishPort(){
  return publish_port;
}

void NESTopologyWorkerNode::setPublishPort(uint16_t publishPort){
  publish_port = publishPort;
}

uint16_t NESTopologyWorkerNode::getReceivePort(){
  return receive_port;
}

uint16_t NESTopologyWorkerNode::getNextFreeReceivePort() {
  receive_port++;
  return receive_port;
}

void NESTopologyWorkerNode::setReceivePort(uint16_t receivePort) {
  receive_port = receivePort;
}

const std::string& NESTopologyWorkerNode::getIp() {
  return this->ip_addr;
}

void NESTopologyWorkerNode::setIp(const std::string &ip) {
  this->ip_addr = ip;
}

}

#include <Topology/NESTopologyCoordinatorNode.hpp>

namespace iotdb{

NESTopologyCoordinatorNode::NESTopologyCoordinatorNode(size_t nodeId, std::string ip_addr) {
  this->node_id = nodeId;
  this->ip_addr = std::move(ip_addr);
  isASink = false;
  cpuCapacity = 0;
  remainingCPUCapacity = 0;
}

size_t NESTopologyCoordinatorNode::getId(){
    return node_id;
  }

  void NESTopologyCoordinatorNode::setId(size_t id){
    node_id = id;
  }


  size_t NESTopologyCoordinatorNode::getCpuCapacity(){
    return cpuCapacity;
  }

  void NESTopologyCoordinatorNode::setCpuCapacity(CPUCapacity cpuCapacity) {
    this->cpuCapacity = cpuCapacity.toInt();
    this->remainingCPUCapacity = this->cpuCapacity;
  }

  void NESTopologyCoordinatorNode::reduceCpuCapacity(size_t usedCapacity){
    assert(usedCapacity <= remainingCPUCapacity);
    this->remainingCPUCapacity = this->remainingCPUCapacity - usedCapacity;
  }

  void NESTopologyCoordinatorNode::increaseCpuCapacity(size_t freedCapacity){
    this->remainingCPUCapacity = this->remainingCPUCapacity + freedCapacity;
  }

  size_t NESTopologyCoordinatorNode::getRemainingCpuCapacity(){
    return remainingCPUCapacity;
  }

  void NESTopologyCoordinatorNode::setSinkNode(bool isASink) {
    this->isASink = isASink;
  }

  bool NESTopologyCoordinatorNode::getIsASinkNode() {
    return this->isASink;
  }

  NESNodeType NESTopologyCoordinatorNode::getEntryType(){
    return Coordinator;
  }

  std::string NESTopologyCoordinatorNode::getEntryTypeString(){
    if (isASink) {
      return "sink";
    }
    return "Coordinator";
  }

  void NESTopologyCoordinatorNode::setQuery(InputQueryPtr pQuery){
    this->query = pQuery;
  }
  ;

  uint16_t NESTopologyCoordinatorNode::getPublishPort(){
    return publish_port;
  }

  void NESTopologyCoordinatorNode::setPublishPort(uint16_t publishPort){
    publish_port = publishPort;
  }

  uint16_t NESTopologyCoordinatorNode::getReceivePort(){
    return receive_port;
  }

  void NESTopologyCoordinatorNode::setReceivePort(uint16_t receivePort){
    receive_port = receivePort;
  }

  uint16_t NESTopologyCoordinatorNode::getNextFreeReceivePort(){
    receive_port++;
    return receive_port;
  }

  const std::string& NESTopologyCoordinatorNode::getIp(){
    return this->ip_addr;
  }

  void NESTopologyCoordinatorNode::setIp(const std::string& ip){
    this->ip_addr = ip;
  }

}

#ifndef INCLUDE_TOPOLOGY_NESTOPOLOGYENTRY_HPP_
#define INCLUDE_TOPOLOGY_NESTOPOLOGYENTRY_HPP_

#include <API/InputQuery.hpp>
#include <NodeEngine/NodeProperties.hpp>
#include <Util/CPUCapacity.hpp>
#include <string>
namespace iotdb {

enum NESNodeType {
  Coordinator,
  Worker,
  Sensor
};

//FIXME:add docu here
class NESTopologyEntry {
 public:
  virtual void setId(size_t id) = 0;

  virtual size_t getId() = 0;

  virtual NESNodeType getEntryType() = 0;

  virtual std::string getEntryTypeString() = 0;

  virtual void setQuery(InputQueryPtr pQuery) = 0;

  virtual int getCpuCapacity() = 0;

  virtual void setCpuCapacity(CPUCapacity cpuCapacity) = 0;

  virtual int getRemainingCpuCapacity() = 0;

  virtual void reduceCpuCapacity(int usedCapacity) = 0;

  virtual void increaseCpuCapacity(int freedCapacity) = 0;

  virtual const std::string &getIp() = 0;

  virtual void setIp(const std::string &ip_addr) = 0;

  /**
   * @brief the publish port is the port on which an actor framework server can be accessed
   * @return port to access CAF
   */
  virtual uint16_t getPublishPort() = 0;

  /**
   * @brief the publish port is the port on which an actor framework server can be accessed
   * @param port to access CAF
   */
  virtual void setPublishPort(uint16_t publishPort) = 0;

  /**
   * @brief the receive port is the port on which internal data transmission via ZMQ is running
   * @return port to access ZMQ
   */
  virtual uint16_t getReceivePort() = 0;

  /**
   * @brief the receive port is the port on which internal data transmission via ZMQ is running
   * @param port to access ZMQ
   */
  virtual void setReceivePort(uint16_t receivePort) = 0;

  //TODO: We need to fix this properly. Currently it just returns the +1 value of the receivePort.
  /**
   * @brief the next free receive port on which internal data transmission via ZMQ is running
   */
  virtual uint16_t getNextFreeReceivePort() = 0;

  void setNodeProperty(std::string nodeProperties) {
    if (nodeProperties != "")
      this->nodeProperties = std::make_shared<NodeProperties>(nodeProperties);
    else
      this->nodeProperties = std::make_shared<NodeProperties>();
  }

  std::string getNodeProperty() {
    return this->nodeProperties->dump();
  }

  std::string toString()
  {

    return "id=" + std::to_string(getId()) + " type=" + getEntryTypeString();
  }
 protected:
  std::string ip_addr;
  uint16_t publish_port;
  uint16_t receive_port;
  NodePropertiesPtr nodeProperties;

};

typedef std::shared_ptr<NESTopologyEntry> NESTopologyEntryPtr;
}  // namespace iotdb
#endif /* INCLUDE_TOPOLOGY_NESTOPOLOGYENTRY_HPP_ */

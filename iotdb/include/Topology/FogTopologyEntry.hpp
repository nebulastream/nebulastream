/*
 * FogTopologyEntry.hpp
 *
 *  Created on: Jan 31, 2019
 *      Author: zeuchste
 */

#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYENTRY_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYENTRY_HPP_

#include <API/InputQuery.hpp>

using std::string;

namespace iotdb {
enum FogNodeType {
  Coordinator, Worker, Sensor
};

class FogTopologyEntry {
 public:
  virtual void setId(size_t id) = 0;

  virtual size_t getId() = 0;

  virtual FogNodeType getEntryType() = 0;

  virtual std::string getEntryTypeString() = 0;

  virtual void setQuery(InputQueryPtr pQuery) = 0;

  virtual int getCpuCapacity() = 0;

  virtual int getRemainingCpuCapacity() = 0;

  virtual void reduceCpuCapacity(int usedCapacity) = 0;

  virtual void increaseCpuCapacity(int freedCapacity) = 0;

  virtual const string &getIp() = 0;

  virtual void setIp(const string &ip_addr) = 0;

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

 protected:
  string ip_addr;
  uint16_t publish_port;
  uint16_t receive_port;
  size_t node_id;
};

typedef std::shared_ptr<FogTopologyEntry> FogTopologyEntryPtr;
} // namespace iotdb
#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYENTRY_HPP_ */

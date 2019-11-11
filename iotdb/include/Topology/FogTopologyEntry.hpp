#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYENTRY_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYENTRY_HPP_
#include <API/InputQuery.hpp>
#include <string>

namespace iotdb {

#define INVALID_NODE_ID INT64_MAX

enum FogNodeType { Worker, Sensor };

class FogTopologyEntry {
 public:

  virtual size_t getId() = 0;
  virtual FogNodeType getEntryType() = 0;
  virtual std::string getEntryTypeString() = 0;
  virtual std::string getIpAddr() = 0;
  virtual int getCpuCapacity() = 0;
  virtual int getRemainingCpuCapacity() = 0;
  virtual void reduceCpuCapacity(int usedCapacity) = 0;
  virtual void increaseCpuCapacity(int freedCapacity) = 0;

  std::string ipAddr;
};

typedef std::shared_ptr<FogTopologyEntry> FogTopologyEntryPtr;
} // namespace iotdb
#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYENTRY_HPP_ */

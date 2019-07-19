/*
 * FogTopologyEntry.hpp
 *
 *  Created on: Jan 31, 2019
 *      Author: zeuchste
 */

#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYENTRY_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYENTRY_HPP_
#include <API/InputQuery.hpp>
#include <string>

namespace iotdb {
enum FogNodeType { Worker, Sensor };

class FogTopologyEntry {
  public:
    virtual void setId(size_t id) = 0;
    virtual size_t getId() = 0;

    virtual FogNodeType getEntryType() = 0;
    virtual std::string getEntryTypeString() = 0;

    virtual void setQuery(InputQueryPtr pQuery) = 0;

    virtual int getCpuCapacity() =0;
    virtual void reduceCpuCapacity(int usedCapacity) =0;
    virtual void increaseCpuCapacity(int freedCapacity) = 0;
};

typedef std::shared_ptr<FogTopologyEntry> FogTopologyEntryPtr;
} // namespace iotdb
#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYENTRY_HPP_ */

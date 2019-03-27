/*
 * RemoteSocketSource.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_REMOTESOCKETSOURCE_H_
#define INCLUDE_REMOTESOCKETSOURCE_H_

#include <Core/TupleBuffer.hpp>
#include <Runtime/DataSource.hpp>

namespace iotdb {

class RemoteSocketSource : public DataSource {
  public:
    RemoteSocketSource(const Schema& schema, int32_t port);
    TupleBufferPtr receiveData();
    const std::string toString() const;

  private:
};
} // namespace iotdb

#endif /* INCLUDE_REMOTESOCKETSOURCE_H_ */

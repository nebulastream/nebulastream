/*
 * RemoteSocketSource.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_REMOTESOCKETSOURCE_H_
#define INCLUDE_REMOTESOCKETSOURCE_H_

#include <Runtime/DataSource.hpp>
#include <Core/TupleBuffer.hpp>

namespace iotdb {

class RemoteSocketSource : public DataSource {
public:
  RemoteSocketSource(const Schema& schema, int32_t port);
  TupleBuffer receiveData();
  const std::string toString() const;
private:
};
}

#endif /* INCLUDE_REMOTESOCKETSOURCE_H_ */

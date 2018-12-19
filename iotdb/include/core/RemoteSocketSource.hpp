/*
 * RemoteSocketSource.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_REMOTESOCKETSOURCE_H_
#define INCLUDE_REMOTESOCKETSOURCE_H_

#include "DataSource.hpp"
#include "TupleBuffer.hpp"
class RemoteSocketSource : public DataSource {
public:
  RemoteSocketSource(int32_t port);
  TupleBuffer receiveData();

private:
};

#endif /* INCLUDE_REMOTESOCKETSOURCE_H_ */

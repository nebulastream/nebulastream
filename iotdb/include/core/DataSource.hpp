/*
 * DataSource.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */
#include "TupleBuffer.hpp"

#ifndef INCLUDE_DATASOURCE_H_
#define INCLUDE_DATASOURCE_H_
#include <thread>

class DataSource {
public:
  DataSource();

  void start();
  void stop();
  void run();

  virtual TupleBuffer receiveData() = 0;
  void submitWork(const TupleBuffer &);

  virtual ~DataSource();

private:
  bool run_thread;
  std::thread thread;

protected:
};
typedef std::shared_ptr<DataSource> DataSourcePtr;

#endif /* INCLUDE_DATASOURCE_H_ */

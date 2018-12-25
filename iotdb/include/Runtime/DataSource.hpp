/*
 * DataSource.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_DATASOURCE_H_
#define INCLUDE_DATASOURCE_H_

#include <thread>
#include <core/TupleBuffer.hpp>
#include <core/Schema.hpp>

namespace iotdb {

class DataSource {
public:
  DataSource(const Schema& schema);

  void start();
  void stop();
  void run();

  virtual TupleBuffer receiveData() = 0;
  virtual const std::string toString() const = 0;
  void submitWork(const TupleBuffer &);
  const Schema& getSchema() const;

  virtual ~DataSource();

private:
  bool run_thread;
  std::thread thread;
protected:
  Schema schema;
};
typedef std::shared_ptr<DataSource> DataSourcePtr;
}

#endif /* INCLUDE_DATASOURCE_H_ */

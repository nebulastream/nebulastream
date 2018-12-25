/*
 * DataSource.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#include <Runtime/DataSource.hpp>
#include <Runtime/Dispatcher.hpp>
#include <iostream>

namespace iotdb {

DataSource::DataSource() : run_thread(false), thread() { std::cout << "Init Data Source!" << std::endl; }

void ass() { std::cout << "Ass" << std::endl; }

DataSource::~DataSource() {
  stop();
  std::cout << "Destroy Data Source " << this << std::endl;
}

void DataSource::start() {
  if (run_thread)
    return;
  run_thread = true;
  std::cout << "Spawn" << std::endl;
  thread = std::thread(std::bind(&DataSource::run, this));
}

void DataSource::stop() {
  if (!run_thread)
    return;
  run_thread = false;
  if (thread.joinable())
    thread.join();
}

void DataSource::run() {
  std::cout << "Running Data Source!" << std::endl;
  while (run_thread) {
    TupleBuffer buf = receiveData();
    std::cout << "Received Data: " << buf.num_tuples << "tuples" << std::endl;
    if (buf.buffer)
      Dispatcher::instance().addWork(buf, this);
    else
      break;
  }
  std::cout << "Data Source Finished!" << std::endl;
}
}

/*
 * DataSource.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#include <Runtime/DataSource.hpp>
#include <Runtime/Dispatcher.hpp>
#include <iostream>
#include <functional>
#include <Runtime/GeneratorSource.hpp>
#include <Runtime/BinarySource.hpp>
#include <Runtime/RemoteSocketSource.hpp>
#include <Util/ErrorHandling.hpp>
#include <cassert>

namespace iotdb {

DataSource::DataSource(const Schema& _schema) : run_thread(false), thread(), schema(_schema) {
  std::cout << "Init Data Source!" << std::endl;
}

const Schema& DataSource::getSchema() const{
  return schema;
}


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


const DataSourcePtr createTestSource(){
  class Functor{
  public:
      Functor() : last_number(0){

      }
      TupleBuffer operator ()(uint64_t generated_tuples, uint64_t num_tuples_to_process){
          TupleBuffer buf = Dispatcher::instance().getBuffer();
          assert(buf.buffer!=NULL);
          uint64_t generated_tuples_this_pass=buf.buffer_size/sizeof(uint64_t);
          std::cout << generated_tuples << ", " << num_tuples_to_process << std::endl;
          generated_tuples_this_pass=std::min(num_tuples_to_process-generated_tuples,generated_tuples_this_pass);

          uint64_t* tuples = (uint64_t*) buf.buffer;
          for(uint64_t i=0;i<generated_tuples_this_pass;i++){
              tuples[i] = last_number++;
          }
          buf.tuple_size_bytes=sizeof(uint64_t);
          buf.num_tuples=generated_tuples_this_pass;
          return buf;
      }

      uint64_t last_number;
  };

  DataSourcePtr source (
        new GeneratorSource<Functor>(
          Schema::create().addField(createField("id", UINT32)),
          100));

  return source;
}

const DataSourcePtr createBinaryFileSource(const Schema& schema, const std::string& path_to_file){
  //instantiate BinaryFileSource
  IOTDB_FATAL_ERROR("Called unimplemented Function");
}

const DataSourcePtr createRemoteTCPSource(const Schema& schema, const std::string& server_ip, int port){
   //instantiate RemoteSocketSource
  IOTDB_FATAL_ERROR("Called unimplemented Function");
}

}

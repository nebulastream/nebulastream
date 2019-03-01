#include <cassert>
#include <functional>
#include <iostream>
#include <memory>

#include <Runtime/BinarySource.hpp>
#include <Runtime/DataSource.hpp>
#include <Runtime/Dispatcher.hpp>
#include <Runtime/GeneratorSource.hpp>
#include <Runtime/RemoteSocketSource.hpp>
#include <Runtime/ZmqSource.hpp>
#include <Util/ErrorHandling.hpp>

namespace iotdb {

DataSource::DataSource(const Schema &_schema) : run_thread(false), thread(), schema(_schema) {
  std::cout << "Init Data Source!" << std::endl;
}

const Schema &DataSource::getSchema() const { return schema; }

DataSource::~DataSource() {
//	stop();
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
//  if (!run_thread)
//    return;
  run_thread = false;
  if (thread.joinable())
    thread.join();
}

void DataSource::run() {
  std::cout << "Running Data Source!" << std::endl;
  size_t cnt = 0;

  while (run_thread) {
    TupleBufferPtr buf = receiveData();
    std::cout << "Received Data: " << buf->num_tuples << "tuples" << std::endl;
    if (buf->buffer && cnt < this->num_buffers_to_process)
    {
        Dispatcher::instance().addWork(buf, this);
        cnt++;
    }
    else
    {
    	run_thread = false;
    	break;
    }
  }
  std::cout << "Data Source Finished!" << std::endl;
}

const DataSourcePtr createTestSource() {
  // Shall this go to the UnitTest Directory in future?
  class Functor {
  public:
    Functor() : one(1) {}
    TupleBufferPtr operator()() {
      //10 tuples of size one
      TupleBufferPtr buf = Dispatcher::instance().getBuffer();
      size_t tupleCnt = buf->buffer_size / sizeof(uint64_t);

      assert(buf->buffer != NULL);

      uint64_t *tuples = (uint64_t *) buf->buffer;
      for (uint64_t i = 0; i < tupleCnt; i++) {
        tuples[i] = one;
      }
      buf->tuple_size_bytes = sizeof(uint64_t);
      buf->num_tuples = tupleCnt;
      return buf;
    }

    uint64_t one;
  };

  DataSourcePtr source(new GeneratorSource<Functor>(Schema::create().addField(createField("id", UINT32)), 1));

  return source;
}

const DataSourcePtr createYSBSource() {
  class Functor {
  public:
	struct __attribute__((packed)) ysbRecord {
	  uint8_t user_id[16];
	  uint8_t page_id[16];
	  uint8_t campaign_id[16];
	  char event_type[9];
	  char ad_type[9];
	  int64_t current_ms;
	  uint32_t ip;

	  ysbRecord(){
		event_type[0] = '-';//invalid record
		current_ms = 0;
		ip = 0;
	  }

	  ysbRecord(const ysbRecord& rhs)
	  {
		memcpy(&user_id, &rhs.user_id, 16);
		memcpy(&page_id, &rhs.page_id, 16);
		memcpy(&campaign_id, &rhs.campaign_id, 16);
		memcpy(&event_type, &rhs.event_type, 9);
		memcpy(&ad_type, &rhs.ad_type, 9);
		current_ms = rhs.current_ms;
		ip = rhs.current_ms;
	  }

	};//size 78 bytes

    Functor() : last_number(0) {}
    TupleBufferPtr operator()() {
      TupleBufferPtr buf = Dispatcher::instance().getBuffer();
      assert(buf->buffer != NULL);
      uint64_t generated_tuples_this_pass = buf->buffer_size / sizeof(ysbRecord);

      uint64_t *tuples = (uint64_t *)buf->buffer;
      for (uint64_t i = 0; i < generated_tuples_this_pass; i++) {
        tuples[i] = last_number++;
      }
      buf->tuple_size_bytes = sizeof(uint64_t);
      buf->num_tuples = generated_tuples_this_pass;
      return buf;
    }

    uint64_t last_number;
  };

  DataSourcePtr source(new GeneratorSource<Functor>(Schema::create().addField(createField("id", UINT32)), 100));

  return source;
}


const DataSourcePtr createZmqSource(const Schema &schema, const std::string &host, const uint16_t port,
                                    const std::string &topic) {
  return std::make_shared<ZmqSource>(schema, host, port, topic);
}

const DataSourcePtr createBinaryFileSource(const Schema &schema, const std::string &path_to_file) {
  // instantiate BinaryFileSource
  IOTDB_FATAL_ERROR("Called unimplemented Function");
}

const DataSourcePtr createRemoteTCPSource(const Schema &schema, const std::string &server_ip, int port) {
  // instantiate RemoteSocketSource
  IOTDB_FATAL_ERROR("Called unimplemented Function");
}

} // namespace iotdb

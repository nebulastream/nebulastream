#include <cassert>
#include <functional>
#include <iostream>
#include <memory>
#include <random>

#include <Runtime/BinarySource.hpp>
#include <Runtime/Dispatcher.hpp>
#include <Runtime/GeneratorSource.hpp>
#include <Runtime/YSBGeneratorSource.hpp>

#include <Runtime/RemoteSocketSource.hpp>
#include <Runtime/ZmqSource.hpp>
#include <Util/ErrorHandling.hpp>
#include <Util/Logger.hpp>

#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <Runtime/DataSource.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::DataSource);


namespace iotdb {


DataSource::DataSource(const Schema &_schema) : run_thread(false), thread(), schema(_schema), generatedTuples(0), generatedBuffers(0) {
  IOTDB_DEBUG("DataSource " << this << ": Init Data Source!")
}

DataSource::DataSource() : run_thread(false), thread(), generatedTuples(0), generatedBuffers(0) {
  IOTDB_DEBUG("DataSource " << this << ": Init Data Source Default!")
}

const Schema &DataSource::getSchema() const { return schema; }

DataSource::~DataSource() {
	stop();
	IOTDB_DEBUG("DataSource " << this << ": Destroy Data Source.")
}

void DataSource::start() {
  if (run_thread)
    return;
  run_thread = true;
  IOTDB_DEBUG("DataSource " << this << ": Spawn thread")
  thread = std::thread(std::bind(&DataSource::run, this));
}

void DataSource::stop() {
	  IOTDB_DEBUG("DataSource " << this << ": Stop called")

//	if (!run_thread)
//	  return;

	run_thread = false;

  if (thread.joinable())
    thread.join();

  IOTDB_DEBUG("DataSource " << this << ": Thread joinded")
}

bool DataSource::isRunning()
{
	return run_thread;
}

void DataSource::run() {
  IOTDB_DEBUG("DataSource " << this << ": Running Data Source")
  size_t cnt = 0;

  while (run_thread) {
	  if(cnt < this->num_buffers_to_process)
	  {
		  TupleBufferPtr buf = receiveData();
		  IOTDB_DEBUG("DataSource " << this << ": Received Data: " << buf->num_tuples << "tuples")
		  if (buf->buffer)
		  {
			  Dispatcher::instance().addWork(buf, this);
			  cnt++;
		  }
		  else
		  {
			  assert(0);
		  }
	  }
	  else
	  {
		  IOTDB_DEBUG("DataSource " << this << ": Stop running")
		run_thread = false;
		break;
	  }
  	}//end of while
  IOTDB_DEBUG("DataSource " << this << ": Data Source Finished")
}

const DataSourcePtr createTestSource() {
  // Shall this go to the UnitTest Directory in future?
  class Functor {
  public:
    Functor() : one(1) {}
    TupleBufferPtr operator()() {
      //10 tuples of size one
      TupleBufferPtr buf = BufferManager::instance().getBuffer();
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


const DataSourcePtr createYSBSource(size_t bufferCnt, size_t campaingCnt, bool preGenerated) {


  Schema schema = Schema::create()
  		.addField("user_id", 16)
  		.addField("page_id", 16)
  		.addField("campaign_id", 16)
  		.addField("event_type", 16)
  		.addField("ad_type", 16)
  		.addField("current_ms", UINT64)
  		.addField("ip", INT32);

//  DataSourcePtr source(new GeneratorSource<Functor>(Schema::create().addField(createField("id", UINT32)), 100));
  DataSourcePtr source(new YSBGeneratorSource(schema, bufferCnt, campaingCnt, preGenerated));

  return source;
}


const DataSourcePtr createZmqSource(const Schema &schema, const std::string &host, const uint16_t port) {
  return std::make_shared<ZmqSource>(schema, host, port);
}

const DataSourcePtr createBinaryFileSource(const Schema &schema, const std::string &path_to_file) {
  // instantiate BinaryFileSource
  IOTDB_FATAL_ERROR("DataSource: Called unimplemented Function");
}

const DataSourcePtr createRemoteTCPSource(const Schema &schema, const std::string &server_ip, int port) {
  // instantiate RemoDataSource: teSocketSource
  IOTDB_FATAL_ERROR("DataSource: Called unimplemented Function");
}

} // namespace iotdb

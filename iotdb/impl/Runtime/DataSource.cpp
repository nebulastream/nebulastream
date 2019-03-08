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
#include <Util/Logger.hpp>

namespace iotdb {

DataSource::DataSource(const Schema &_schema) : run_thread(false), thread(), schema(_schema) {
  IOTDB_DEBUG("DataSource: Init Data Source!")
}

const Schema &DataSource::getSchema() const { return schema; }

DataSource::~DataSource() {
//	stop();
	IOTDB_DEBUG("DataSource: Destroy Data Source.")
}

void DataSource::start() {
  if (run_thread)
    return;
  run_thread = true;
  IOTDB_DEBUG("DataSource: Spawn thread")
  thread = std::thread(std::bind(&DataSource::run, this));
}

void DataSource::stop() {
//  if (!run_thread)
//    return;
  run_thread = false;
  if (thread.joinable())
    thread.join();
}

bool DataSource::isRunning()
{
	return run_thread;
}
void DataSource::run() {
  IOTDB_DEBUG("DataSource: Running Data Source")
  size_t cnt = 0;

  while (run_thread) {
    TupleBufferPtr buf = receiveData();
    IOTDB_DEBUG("DataSource: Received Data: " << buf->num_tuples << "tuples")
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
  IOTDB_DEBUG("DataSource: Data Source Finished")
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

struct __attribute__((packed)) ysbRecord {
	  char user_id[16];
	  char page_id[16];
	  char campaign_id[16];
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

void generateTuple(ysbRecord* data, size_t campaingOffset, uint64_t campaign_lsb, uint64_t campaign_msb, size_t event_id)
{
		std::string events[] = {"view", "click", "purchase"};
		size_t currentID  = event_id % 3;

	  memcpy(data->campaign_id, &campaign_msb, 8);

	  uint64_t campaign_lsbr = campaign_lsb + campaingOffset;
	  memcpy(&data->campaign_id[8], &campaign_lsbr, 8);

	  const char* str = events[currentID].c_str();
	  strcpy(&data->ad_type[0], "banner78");
	  strcpy(&data->event_type[0], str);

	  auto ts = std::chrono::system_clock::now().time_since_epoch();
	  data->current_ms = std::chrono::duration_cast<std::chrono::milliseconds>(ts).count();

	  data->ip = event_id;
}

void generate(ysbRecord* data, size_t generated_tuples_this_pass)
{
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen; //(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<size_t> diss(0, SIZE_MAX);

	const size_t campaingCnt = 10;

	size_t randomCnt = generated_tuples_this_pass/10;
	size_t* randomNumbers = new size_t[randomCnt];
	std::uniform_int_distribution<size_t> disi(0, campaingCnt);
	for(size_t i = 0; i < randomCnt; i++)
		randomNumbers[i] = disi(gen);

	uint64_t campaign_lsb, campaign_msb;
	auto uuid = diss(gen);
	uint8_t* uuid_ptr = reinterpret_cast<uint8_t*>(&uuid);
	memcpy(&campaign_msb, uuid_ptr, 8);
	memcpy(&campaign_lsb, uuid_ptr + 8, 8);
	campaign_lsb &= 0xffffffff00000000;

	for(size_t u = 0; u < generated_tuples_this_pass; u++)
	{
		generateTuple(&data[u], /**campaingOffset*/ randomNumbers[u%randomCnt], campaign_lsb, campaign_msb, /**eventID*/ u);
	}
}


const DataSourcePtr createYSBSource(size_t bufferCnt) {
  class Functor {
  public:

    Functor() : last_number(0) {}
    TupleBufferPtr operator()() {
      TupleBufferPtr buf = Dispatcher::instance().getBuffer();
      assert(buf->buffer != NULL);
      uint64_t generated_tuples_this_pass = buf->buffer_size / sizeof(ysbRecord);

      generate((ysbRecord*) buf->buffer, generated_tuples_this_pass);

      buf->tuple_size_bytes = sizeof(ysbRecord);
      buf->num_tuples = generated_tuples_this_pass;
      return buf;
    }

    uint64_t last_number;
  };

//  DataSourcePtr source(new GeneratorSource<Functor>(Schema::create().addField(createField("id", UINT32)), 100));
  DataSourcePtr source(new GeneratorSource<Functor>(Schema::create().addField(createField("id", UINT32)), bufferCnt));

  return source;
}


const DataSourcePtr createZmqSource(const Schema &schema, const std::string &host, const uint16_t port,
                                    const std::string &topic) {
  return std::make_shared<ZmqSource>(schema, host, port, topic);
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

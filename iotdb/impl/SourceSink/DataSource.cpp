#include <cassert>
#include <functional>
#include <iostream>
#include <memory>
#include <random>

#include <NodeEngine/Dispatcher.hpp>
#include <Util/ErrorHandling.hpp>
#include <Util/Logger.hpp>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>

#include <SourceSink/DataSource.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::DataSource);

namespace iotdb {

DataSource::DataSource(const Schema &_schema)
    : running(false),
      thread(),
      schema(_schema),
      generatedTuples(0),
      generatedBuffers(0),
      num_buffers_to_process(UINT64_MAX) {
  IOTDB_DEBUG("DataSource " << this << ": Init Data Source with schema")
}

DataSource::DataSource()
    : running(false),
      thread(),
      generatedTuples(0),
      generatedBuffers(0),
      num_buffers_to_process(UINT64_MAX) {
  IOTDB_DEBUG("DataSource " << this << ": Init Data Source Default w/o schema")
}

const Schema &DataSource::getSchema() const {
  return schema;
}

DataSource::~DataSource() {
  stop();
  IOTDB_DEBUG("DataSource " << this << ": Destroy Data Source.")
}

bool DataSource::start() {
  if (running)
    return false;
  running = true;

  IOTDB_DEBUG("DataSource " << this << ": Spawn thread")
  thread = std::thread(std::bind(&DataSource::running_routine, this));
  return true;
}

bool DataSource::stop() {
  IOTDB_DEBUG("DataSource " << this << ": Stop called")
  running = false;

  if (thread.joinable()) {
    thread.detach();
    IOTDB_DEBUG("DataSource " << this << ": Thread joinded")
    return true;
  } else {
    IOTDB_DEBUG("DataSource " << this << ": Thread is not joinable")
  }
  return false;
}

bool DataSource::isRunning() {
  return running;
}

void DataSource::running_routine() {
  IOTDB_DEBUG("DataSource " << this << ": Running Data Source")
  size_t cnt = 0;

  while (running) {
    if (cnt < num_buffers_to_process) {
      TupleBufferPtr buf = receiveData();
      if (buf) {
        IOTDB_DEBUG(
            "DataSource " << this << ": Received Data: " << buf->getNumberOfTuples() << "tuples")
        if (buf->getBuffer()) {
          Dispatcher::instance().addWork(buf, this);
          cnt++;
        } else {
          IOTDB_DEBUG("DataSource " << this << ": Received buffer is invalid")
          assert(0);
        }
      } else {
        IOTDB_DEBUG("DataSource " << this << ": Receiving thread terminated ... stopping")
        running = false;
        break;
      }
    } else {
      IOTDB_DEBUG("DataSource " << this << ": All buffers processed ... stopping")
      running = false;
      break;
    }
  }
  IOTDB_DEBUG("DataSource " << this << ": Data Source finished processing")
}

// debugging
void DataSource::setNumBuffersToProcess(size_t cnt) {
  num_buffers_to_process = cnt;
};
size_t DataSource::getNumberOfGeneratedTuples() {
  return generatedTuples;
};
size_t DataSource::getNumberOfGeneratedBuffers() {
  return generatedBuffers;
};
const std::string DataSource::getSourceSchemaAsString() {
  return schema.toString();
};
}  // namespace iotdb

#include <cassert>
#include <functional>
#include <iostream>
#include <memory>
#include <random>

#include <NodeEngine/Dispatcher.hpp>
#include <Util/Logger.hpp>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>

#include <SourceSink/MultiDataSource.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(NES::MultiDataSource);

namespace NES {

MultiDataSource::MultiDataSource()
    : running(false),
      thread(),
      generatedTuples(0),
      generatedBuffers(0),
      num_buffers_to_process(UINT64_MAX) {
  NES_DEBUG("MultiDataSource " << this << ": Init Data Source Default w/o schema")
}

MultiDataSource::~MultiDataSource() {
  stop();
  NES_DEBUG("MultiDataSource " << this << ": Destroy Data Source.")
}

bool MultiDataSource::start() {
  if (running)
    return false;
  running = true;

  NES_DEBUG("MultiDataSource " << this << ": Spawn thread")
  thread = std::thread(std::bind(&MultiDataSource::running_routine, this));
  return true;
}

bool MultiDataSource::stop() {
  NES_DEBUG("MultiDataSource " << this << ": Stop called")
  running = false;

  if (thread.joinable()) {
    thread.detach();
    NES_DEBUG("MultiDataSource " << this << ": Thread joinded")
    return true;
  } else {
    NES_DEBUG("MultiDataSource " << this << ": Thread is not joinable")
  }
  return false;
}

bool MultiDataSource::isRunning() {
  return running;
}

void MultiDataSource::running_routine() {
  NES_DEBUG("MultiDataSource " << this << ": Running Data Source")
  size_t cnt = 0;

  while (running) {
    if (cnt < num_buffers_to_process) {
      TupleBufferPtr buf = receiveData();
      if (buf) {
        NES_DEBUG(
            "MultiDataSource " << this << ": Received Data: " << buf->getNumberOfTuples() << "tuples")
        if (buf->getBuffer()) {
          Dispatcher::instance().addWork(buf, this);
          cnt++;
        } else {
          NES_DEBUG("MultiDataSource " << this << ": Received buffer is invalid")
          assert(0);
        }
      } else {
        NES_DEBUG("MultiDataSource " << this << ": Receiving thread terminated ... stopping")
        running = false;
        break;
      }
    } else {
      NES_DEBUG("MultiDataSource " << this << ": All buffers processed ... stopping")
      running = false;
      break;
    }
  }
  NES_DEBUG("MultiDataSource " << this << ": Data Source finished processing")
}

// debugging
void MultiDataSource::setNumBuffersToProcess(size_t cnt) {
  num_buffers_to_process = cnt;
};
size_t MultiDataSource::getNumberOfGeneratedTuples() {
  return generatedTuples;
};
size_t MultiDataSource::getNumberOfGeneratedBuffers() {
  return generatedBuffers;
};
}  // namespace NES

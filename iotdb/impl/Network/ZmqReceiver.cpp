#include <Network/ZmqReceiver.hpp>

#include <cassert>
#include <cstdint>
#include <cstring>
#include <sstream>
#include <string>
#include <zmq.hpp>

#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/Dispatcher.hpp>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <Util/Logger.hpp>
namespace NES {

ZmqReceiver::ZmqReceiver()
    : host(""),
      port(0),
      isReady(false),
      context(zmq::context_t(1)),
      socket(zmq::socket_t(context, ZMQ_PULL)) {
  //This constructor is needed for Serialization
}

ZmqReceiver::ZmqReceiver(const std::string &host, const uint16_t port)
    : DataSource(schema),
      host(host),
      port(port),
      isReady(false),
      context(zmq::context_t(1)),
      socket(zmq::socket_t(context, ZMQ_PULL)) {
  NES_DEBUG(
      "ZmqReceiver  " << this << ": Init ZMQ ZmqReceiver to " << host << ":" << port << "/")
}

ZmqReceiver::~ZmqReceiver() {
  bool success = teardown();
  if (success) {
    NES_DEBUG("ZmqReceiver  " << this << ": Destroy ZMQ Source")
  } else {
    NES_ERROR(
        "ZmqReceiver  " << this << ": Destroy ZMQ Source failed cause it could not be disconnected")
    assert(0);
  }
  NES_DEBUG("ZmqReceiver  " << this << ": Destroy ZMQ Source")
}

TupleBufferPtr ZmqReceiver::receiveData() {
  NES_DEBUG("ZmqReceiver  " << this << ": receiveData ")
  if (isReady) {
    try {
      // Receive new chunk of data
      zmq::message_t new_data;
      socket.recv(&new_data); // envelope - not needed at the moment
      size_t tupleCnt = *((size_t *) new_data.data());
      NES_DEBUG("ZmqReceiver received #tups " << tupleCnt)

      zmq::message_t new_data2;
      socket.recv(&new_data2); // actual data

      // Get some information about received data
      size_t tuple_size = schema.getSchemaSize();
      // Create new TupleBuffer and copy data
      TupleBufferPtr buffer = BufferManager::instance().getBuffer();
      NES_DEBUG("ZmqReceiver  " << this << ": got buffer ")

      // TODO: If possible only copy the content not the empty part
      std::memcpy(buffer->getBuffer(), new_data2.data(), buffer->getBufferSizeInBytes());
      buffer->setNumberOfTuples(tupleCnt);
      buffer->setTupleSizeInBytes(tuple_size);

      if (buffer->getBufferSizeInBytes() == new_data2.size()) {
        NES_WARNING("ZmqReceiver  " << this << ": return buffer ")
      }

      return buffer;
    }
    catch (const zmq::error_t &ex) {
      // recv() throws ETERM when the zmq context is destroyed,
      //  as when AsyncZmqListener::Stop() is called
      if (ex.num() != ETERM) {
        NES_ERROR("ZmqReceiver: " << ex.what())
      }
    }
  } else {
    NES_ERROR("ZmqReceiver: Not connected!")
  }
  return nullptr;
}

const std::string ZmqReceiver::toString() const {
  std::stringstream ss;
  ss << "ZMQ_SOURCE(";
  ss << "SCHEMA(" << schema.toString() << "), ";
  ss << "HOST=" << host << ", ";
  ss << "PORT=" << port << ", ";
  return ss.str();
}

bool ZmqReceiver::setup() {
  if (!isReady) {
    auto address = std::string("tcp://") + host + std::string(":") + std::to_string(port);

    try {
      socket.setsockopt(ZMQ_LINGER, 0);
      socket.bind(address.c_str());
      isReady = true;
    }
    catch (const zmq::error_t &ex) {
      // recv() throws ETERM when the zmq context is destroyed,
      //  as when AsyncZmqListener::Stop() is called
      if (ex.num() != ETERM) {
        NES_ERROR("ZmqReceiver: " << ex.what())
      }
      isReady = false;
    }
  }
  if (isReady) {
    NES_DEBUG("ZmqReceiver  " << this << ": connected")
  } else {
    NES_DEBUG("Exception: ZmqReceiver  " << this << ": NOT connected")
  }
  return isReady;
}

bool ZmqReceiver::teardown() {
  if (isReady) {
    socket.close();
    isReady = false;
    NES_DEBUG("ZmqReceiver  " << this << ": socket closed successfully")
    return true;
  }
  else {
    NES_DEBUG("ZmqReceiver  " << this << ": Tear down failed! No open socket found.")
  }
  return false;
}

SourceType ZmqReceiver::getType() const {
  return ZMQ_SOURCE;
}

}  // namespace NES

BOOST_CLASS_EXPORT(NES::ZmqReceiver);

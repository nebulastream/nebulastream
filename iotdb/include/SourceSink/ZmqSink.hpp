#ifndef ZMQSINK_HPP
#define ZMQSINK_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <zmq.hpp>

#include <SourceSink/DataSink.hpp>

namespace iotdb {

/**
 * @brief this class provide a zmq as data sink
 */
class ZmqSink : public DataSink {

 public:
  /**
   * @brief constructor for the zmq sink
   * @param schema of the output buffer
   * @param host name of the target queue
   * @param port of the target queue
   */
  ZmqSink(const Schema& schema, const std::string& host, const uint16_t port);

  /**
   * @brief destructor of zmq sink that disconnects the queue before deconstruction
   * @note if queue cannot be disconnected, an assertion is raised
   */
  ~ZmqSink() override;

  /**
   * @brief method to write data into the sink
   * @param TupleBufferPtr to the buffer to write
   * @return bool indicating the success of the write operation
   */
  bool writeData(const TupleBufferPtr input_buffer);

  /**
   * @brief setup method for zmq sink
   * @Note required due to derivation but does nothing
   */
  void setup() override;

  /**
   * @brief shutdown method for zmq sink
   * @Note required due to derivation but does nothing
   */
  void shutdown() override;

  /**
   * @brief override the toString method for the zmq sink
   * @return returns string describing the zmq sink
   */
  const std::string toString() const override;

 private:
  /**
   * @brief method to connect zmq using the host and port specified before
   * check if already connected, if not connect try to connect, if already connected return
   * @return bool indicating if connection could be established
   */
  bool connect();

  /**
   * @brief method to disconnect zmq
   * check if already disconnected, if not disconnected try to disconnect, if already disconnected return
   * @return bool indicating if connection could be established
   */
  bool disconnect();

  /**
   * @brief default constructor required by boost serialize
   */
  ZmqSink();

  /**
   * @brief method for serialization, all listed variable below are added to the
   * serialization/deserialization process
   */
  friend class boost::serialization::access;
  template<class Archive> void serialize(Archive& ar,
                                         const unsigned int version) {
    ar & boost::serialization::base_object<DataSink>(*this);
    ar & host;
    ar & port;
  }

  std::string host;
  uint16_t port;
  size_t tupleCnt;

  bool connected;
  zmq::context_t context;
  zmq::socket_t socket;
};
}  // namespace iotdb
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::ZmqSink)
#endif // ZMQSINK_HPP

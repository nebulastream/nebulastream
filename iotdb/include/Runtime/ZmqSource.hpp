#ifndef ZMQSOURCE_HPP
#define ZMQSOURCE_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <zmq.hpp>

#include <Core/TupleBuffer.hpp>
#include <Runtime/DataSource.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
namespace iotdb {

class ZmqSource : public DataSource {

  public:
    ZmqSource(const Schema& schema, const std::string& host, const uint16_t port);
    ~ZmqSource();

    TupleBufferPtr receiveData() override;
    const std::string toString() const override;

  private:
    ZmqSource();
    friend class boost::serialization::access;
    template <class Archive> void serialize(Archive& ar, const unsigned int version)
    {
        ar& boost::serialization::base_object<DataSource>(*this);
        ar& host;
        ar& port;
    }
    std::string host;
    uint16_t port;
    bool connected;
    zmq::context_t context;
    zmq::socket_t socket;

    bool connect();
    bool disconnect();
};
} // namespace iotdb
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::ZmqSource)
#endif // ZMQSOURCE_HPP

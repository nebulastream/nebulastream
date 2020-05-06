#include <SourceSink/ZmqSource.hpp>

#include <cassert>
#include <cstdint>
#include <cstring>
#include <sstream>
#include <string>
#include <zmq.hpp>

#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/QueryManager.hpp>

#include <Util/Logger.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

namespace NES {

ZmqSource::ZmqSource()
    : host(""), port(0), connected(false), context(zmq::context_t(1)), socket(zmq::socket_t(context, ZMQ_PULL)) {
    // This constructor is needed for Serialization
    NES_DEBUG("ZMQSOURCE  " << this << ": Init DEFAULT for serializazionZMQ ZMQSOURCE to " << host << ":" << port << "/");
}

ZmqSource::ZmqSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const std::string& host, const uint16_t port)
    : DataSource(schema, bufferManager, queryManager), host(host), port(port), connected(false), context(zmq::context_t(1)),
      socket(zmq::socket_t(context, ZMQ_PULL)) {
    NES_DEBUG("ZMQSOURCE  " << this << ": Init ZMQ ZMQSOURCE to " << host << ":" << port << "/");
}

ZmqSource::~ZmqSource() {
    bool success = disconnect();
    if (success) {
        NES_DEBUG("ZMQSOURCE  " << this << ": Destroy ZMQ Source");
    } else {
        NES_ERROR("ZMQSOURCE  " << this << ": Destroy ZMQ Source failed cause it could not be disconnected");
        assert(0);
    }
    NES_DEBUG("ZMQSOURCE  " << this << ": Destroy ZMQ Source");
}

std::optional<TupleBuffer> ZmqSource::receiveData() {
    NES_DEBUG("ZMQSource  " << this << ": receiveData ");
    if (connect()) {// was if (connect()) {
        try {
            // Receive new chunk of data
            zmq::message_t new_data;
            socket.recv(&new_data);// envelope - not needed at the moment
            size_t tupleCnt = *((size_t*) new_data.data());
            NES_DEBUG("ZMQSource received #tups " << tupleCnt);

            zmq::message_t new_data2;
            socket.recv(&new_data2);// actual data

            // Get some information about received data
            size_t tuple_size = schema->getSchemaSizeInBytes();
            // Create new TupleBuffer and copy data
            auto buffer = bufferManager->getBufferBlocking();
            NES_DEBUG("ZMQSource  " << this << ": got buffer ");

            // TODO: If possible only copy the content not the empty part
            std::memcpy(buffer.getBuffer(), new_data2.data(), buffer.getBufferSize());
            buffer.setNumberOfTuples(tupleCnt);
            buffer.setTupleSizeInBytes(tuple_size);

            if (buffer.getBufferSize() == new_data2.size()) {
                NES_WARNING("ZMQSource  " << this << ": return buffer ");
            }

            return buffer;
        } catch (const zmq::error_t& ex) {
            // recv() throws ETERM when the zmq context is destroyed,
            //  as when AsyncZmqListener::Stop() is called
            if (ex.num() != ETERM) {
                NES_ERROR("ZMQSOURCE: " << ex.what());
            }
        }
    } else {
        NES_ERROR("ZMQSOURCE: Not connected!");
        throw new std::runtime_error("ZMQSOURCE: Not connected!");
    }
}

const std::string ZmqSource::toString() const {
    std::stringstream ss;
    ss << "ZMQ_SOURCE(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    ss << "HOST=" << host << ", ";
    ss << "PORT=" << port << ", ";
    return ss.str();
}

bool ZmqSource::connect() {
    if (!connected) {
        NES_DEBUG("ZMQSOURCE was !conncect now connect " << this << ": connected");
        if (host == "localhost") {
            host = "*";
        }
        auto address = std::string("tcp://") + host + std::string(":") + std::to_string(port);
        NES_DEBUG("ZMQSOURCE use address " << address);
        try {
            socket.setsockopt(ZMQ_LINGER, 0);
            socket.bind(address.c_str());
            NES_DEBUG("ZMQSOURCE  " << this << ": set connected true");
            connected = true;
        } catch (const zmq::error_t& ex) {
            // recv() throws ETERM when the zmq context is destroyed,
            //  as when AsyncZmqListener::Stop() is called
            if (ex.num() != ETERM) {
                NES_ERROR("ZMQSOURCE ERROR: " << ex.what());
                NES_DEBUG("ZMQSOURCE  " << this << ": set connected false");
            }
            connected = false;
        }
    }

    if (connected) {
        NES_DEBUG("ZMQSOURCE  " << this << ": connected");
    } else {
        NES_DEBUG("Exception: ZMQSOURCE  " << this << ": NOT connected");
    }
    return connected;
}

bool ZmqSource::disconnect() {
    if (connected) {
        socket.close();
        connected = false;
    }
    if (!connected) {
        NES_DEBUG("ZMQSOURCE  " << this << ": disconnected");
    } else {
        NES_DEBUG("ZMQSOURCE  " << this << ": NOT disconnected");
    }
    return !connected;
}

SourceType ZmqSource::getType() const { return ZMQ_SOURCE; }

const std::string& ZmqSource::getHost() const {
    return host;
}

uint16_t ZmqSource::getPort() const {
    return port;
}

} // namespace NES

BOOST_CLASS_EXPORT(NES::ZmqSource);

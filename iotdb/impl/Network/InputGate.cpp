#include <Network/InputGate.hpp>

#include <zmq.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <Util/Logger.hpp>
#include <Network/PacketHeader.hpp>
#include <Util/SerializationTools.hpp>
#include <NodeEngine/Dispatcher.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

InputGate::InputGate(std::string host, const uint16_t port) : host(std::move(host)),
                                                              port(port),
                                                              connected(false),
                                                              context(zmq::context_t(1)),
                                                              socket(zmq::socket_t(context, ZMQ_PULL)) {
}

std::tuple<std::string, TupleBufferPtr> InputGate::receiveData() {
    try {
        // Receive new chunk of data
        NES_DEBUG("InputGate: Waiting to receive packet..")

        zmq::message_t envelope;
        socket.recv(&envelope);

        std::string serPacketHeader = std::string(static_cast<char*>(envelope.data()), envelope.size());
        PacketHeader pH = SerializationTools::parsePacketHeader(serPacketHeader);

        NES_DEBUG("InputGate: Received packet from source " << pH.getSourceId())
        zmq::message_t tupleData;
        socket.recv(&tupleData);
        TupleBufferPtr buffer = BufferManager::instance().getBuffer();

        if (buffer->getBufferSizeInBytes() == tupleData.size()) {
            NES_ERROR("InputGate:  " << this << " packet size" << pH.getSourceId()
                                       << " does not match with local buffer size!")
        }
        std::memcpy(buffer->getBuffer(), tupleData.data(), tupleData.size());

        buffer->setNumberOfTuples(pH.getTupleCount());
        buffer->setTupleSizeInBytes(pH.getTupleSize());

        NES_DEBUG("InputGate: Adding work to Dispatcher for " << pH.getSourceId())
        //Dispatcher::instance().addWork(pH.getSourceId(), buffer);
        return std::make_tuple(pH.getSourceId(), buffer);
    }
    catch (const zmq::error_t& ex) {
        // recv() throws ETERM when the zmq context is destroyed,
        //  as when AsyncZmqListener::Stop() is called
        if (ex.num() != ETERM) {
            NES_ERROR("InputGate: Error during receive: " << ex.what())
        }
    }
    NES_INFO("InputGate: Socket closed for receiving data!")
}

bool InputGate::setup() {
    try {
        auto address = std::string("tcp://") + host + std::string(":") + std::to_string(port);
        socket.setsockopt(ZMQ_LINGER, 0);
        socket.bind(address.c_str());
        connected = true;
        return true;
    }
    catch (const zmq::error_t& ex) {
        NES_ERROR("InputGate: Error during setup(): " << ex.what())

        connected = false;
        return false;
    }
}

bool InputGate::close() {
    connected = false;
    try {
        socket.close();
        NES_INFO("InputGate:  " << this << " closed socket on port " << port << " successfully!")
        return true;
    }
    catch (const zmq::error_t& ex) {
        NES_ERROR("InputGate: Error during close(): " << ex.what())
        return false;
    }
}

bool InputGate::isReady() {
    return connected;
}

string InputGate::getHost() const {
    char endpoint[1024];
    size_t size = sizeof(endpoint);
    socket.getsockopt( ZMQ_LAST_ENDPOINT, &endpoint, &size );
    std::string sEndpoint(endpoint);
    return UtilityFunctions::getStringBetweenTwoDelimiters(sEndpoint, "://", ":");
}

uint16_t InputGate::getPort() const {
    char endpoint[1024];
    size_t size = sizeof(endpoint);
    socket.getsockopt( ZMQ_LAST_ENDPOINT, &endpoint, &size );
    std::string sEndpoint(endpoint);
    uint16_t parsed = std::stoi(sEndpoint.substr(sEndpoint.find_last_of(':')+1));
    return parsed;
}

}  // namespace NES

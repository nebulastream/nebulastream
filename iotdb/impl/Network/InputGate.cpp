#include <Network/InputGate.hpp>

#include <zmq.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <Util/Logger.hpp>
#include <Network/PacketHeader.hpp>
#include <Util/SerializationTools.hpp>
#include <NodeEngine/Dispatcher.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

InputGate::InputGate(std::string zmqHost, const uint16_t zmqPort) : zmqHost(std::move(zmqHost)),
                                                                    zmqPort(zmqPort),
                                                                    connected(false),
                                                                    context(zmq::context_t(1)),
                                                                    socket(zmq::socket_t(context, ZMQ_PULL)) {
    //initializes a zmq context with 1 io_thread
    //initializes a zmq socket according to the push/pull design to avoid data duplication
}

std::tuple<std::string, TupleBufferPtr> InputGate::receiveData() {
    try {
        // Receive new chunk of data
        NES_DEBUG("InputGate: Waiting to receive packet..")

        zmq::message_t envelope;
        socket.recv(&envelope);

        NES_DEBUG("InputGate: Received packetHeader with size " << envelope.size())

        if (envelope.size() <= 0) {
            throw std::invalid_argument("InputGate: PacketHeader is empty");
        }

        std::string serializedPacketHeader = std::string(static_cast<char*>(envelope.data()), envelope.size());
        PacketHeader packetHeader = SerializationTools::parsePacketHeader(serializedPacketHeader);

        NES_DEBUG("InputGate: Parsed packet " << packetHeader.toString())
        zmq::message_t tupleData;
        socket.recv(&tupleData);
        TupleBufferPtr buffer = BufferManager::instance().getFixSizeBuffer();

        if (!buffer || buffer->getBufferSizeInBytes() <= tupleData.size()) {
            NES_ERROR("InputGate:  " << " invalid buffer size received " << buffer->getBufferSizeInBytes()
            << " for source packet " << packetHeader.getSourceId() << " with size " << tupleData.size())
            throw std::runtime_error("InputGate: Invalid buffer for packet " + packetHeader.getSourceId());
        }

        std::memcpy(buffer->getBuffer(), tupleData.data(), tupleData.size());
        buffer->setNumberOfTuples(packetHeader.getTupleCount());
        buffer->setTupleSizeInBytes(packetHeader.getTupleSize());

        return std::make_tuple(packetHeader.getSourceId(), buffer);
    }
    catch (const zmq::error_t& ex) {
        // recv() throws ETERM when the zmq context is destroyed,
        //  as when AsyncZmqListener::Stop() is called
        if (ex.num() != ETERM) {
            NES_ERROR("InputGate: Error during receive: " << ex.what())
            throw ex;
        }
    }
    NES_INFO("InputGate: Socket closed for receiving data!")
}

bool InputGate::setup() {
    try {
        auto address = std::string("tcp://") + zmqHost + std::string(":") + std::to_string(zmqPort);
        // set linger period for socket shutdown
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
    catch (const std::exception& ex){
        NES_ERROR("InputGate: Error during setup(): " << ex.what())
        throw ex;
    }
}

bool InputGate::close() {
    try {
        socket.close();
        connected = false;
        NES_INFO("InputGate:  " << this << " closed socket on port " << zmqPort << " successfully!")
        return true;
    }
    catch (const zmq::error_t& ex) {
        NES_ERROR("InputGate: Error during close(): " << ex.what())
        return false;
    }
    catch (const std::exception& ex){
        NES_ERROR("InputGate: Error during setup(): " << ex.what())
        throw ex;
    }
}

bool InputGate::isConnected() {
    return connected;
}

string InputGate::getHost() const {
    char endpoint[1024];
    size_t size = sizeof(endpoint);
    //retrieve endpoint information from zmq connection. Especially needed when ephemeral port is used
    socket.getsockopt(ZMQ_LAST_ENDPOINT, &endpoint, &size);
    std::string sEndpoint(endpoint);
    NES_DEBUG("InputGate: Retrieving host from zmq endpoint " << sEndpoint)
    return UtilityFunctions::getFirstStringBetweenTwoDelimiters(sEndpoint, "://", ":");
}

uint16_t InputGate::getPort() const {
    char endpoint[1024];
    size_t size = sizeof(endpoint);
    //retrieve endpoint information from zmq connection. Especially needed when ephemeral port is used
    socket.getsockopt(ZMQ_LAST_ENDPOINT, &endpoint, &size);
    std::string sEndpoint(endpoint);
    NES_DEBUG("InputGate: Retrieving port from zmq endpoint " << sEndpoint)
    uint16_t parsed = std::stoi(sEndpoint.substr(sEndpoint.find_last_of(':') + 1));
    return parsed;
}

}  // namespace NES

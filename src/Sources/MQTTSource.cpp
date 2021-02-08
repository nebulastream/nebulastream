/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifdef ENABLE_MQTT_BUILD

#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Sources/MQTTSource.hpp>
#include <Util/Logger.hpp>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <mqtt/async_client.h>
#include <sstream>
#include <string>

using namespace std;
using namespace std::chrono;

namespace NES {

MQTTSource::MQTTSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
                       const std::string serverAddress, const std::string clientId, const std::string user,
                       const std::string topic, OperatorId operatorId)
    : DataSource(schema, bufferManager, queryManager, operatorId), connected(false), serverAddress(serverAddress),
      clientId(clientId), user(user), topic(topic), client(serverAddress, clientId) {
    NES_DEBUG("MQTTSource  " << this << ": Init MQTTSource to " << serverAddress << " with client id: " << clientId << " and ");
}

MQTTSource::~MQTTSource() {
    NES_DEBUG("MQTTSource::~MQTTSource()");
    bool success = disconnect();
    if (success) {
        NES_DEBUG("MQTTSource  " << this << ": Destroy MQTT Source");
    } else {
        NES_ERROR("MQTTSource  " << this << ": Destroy MQTT Source failed cause it could not be disconnected");
        assert(0);
    }
    NES_DEBUG("MQTTSource  " << this << ": Destroy MQTT Source");
}

std::optional<NodeEngine::TupleBuffer> MQTTSource::receiveData() {
    NES_DEBUG("MQTTSource  " << this << ": receiveData ");

    if (connect()) {// was if (connect()) {
        try {
            NES_INFO("Waiting for messages on topic: '" << topic << "'");

            auto buffer = bufferManager->getBufferBlocking();
            //ToDo: check if only one tuple send each time or changes possible
            int count = 0;
            while (true) {
                auto msg = client.consume_message();
                if (!msg) {
                    buffer.setNumberOfTuples(count);
                    break;
                }
                std::memcpy(buffer.getBuffer(), msg->get_payload_str().c_str(), msg->get_payload().size());
            }
            return buffer;
        } catch (const mqtt::exception& exc) {
            NES_ERROR("MQTTSource error: " << exc.what());
            return std::nullopt;
        } catch (...) {
            NES_ERROR("MQTTSource general error");
            return std::nullopt;
        }
    } else {
        NES_ERROR("MQTTSource: Not connected!");
        return std::nullopt;
    }
}

const std::string MQTTSource::toString() const {
    std::stringstream ss;
    ss << "MQTTSOURCE(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    ss << "SERVERADDRESS=" << serverAddress << ", ";
    ss << "CLIENTID=" << clientId << ", ";
    ss << "USER=" << user << ", ";
    ss << "TOPIC=" << topic << ". ";
    return ss.str();
}

bool MQTTSource::connect() {
    if (!connected) {
        NES_DEBUG("MQTTSource was !connect now connect " << this << ": connected");
        // connect with user name and password
        auto connOpts = mqtt::connect_options_builder()
                            .user_name(user)
                            .keep_alive_interval(seconds(1))//waiting interval in standard chrono notation
                            .clean_session(false)
                            .finalize();
        try {
            // Start consumer before connecting to make sure to not miss messages
            client.start_consuming();

            // Connect to the server
            NES_INFO("MQTTSource: Connecting to the MQTT server...");
            auto tok = client.connect(connOpts);

            // Getting the connect response will block waiting for the
            // connection to complete.
            auto rsp = tok->get_connect_response();

            // If there is no session present, then we need to subscribe, but if
            // there is a session, then the server remembers us and our
            // subscriptions.
            if (!rsp.is_session_present())
                client.subscribe(topic, 1)->wait();
            connected = client.is_connected();
        } catch (const mqtt::exception& exc) {
            NES_WARNING("\n  " << exc);
            connected = false;
            return connected;
        }

        if (connected) {
            NES_INFO("MQTTSource: Connection established with topic: " << topic);
            NES_DEBUG("MQTTSource  " << this << ": connected");
        } else {
            NES_DEBUG("Exception: MQTTSource  " << this << ": NOT connected");
        }
    }
    return connected;
}

bool MQTTSource::disconnect() {
    NES_DEBUG("MQTTSource::disconnect() connected=" << connected);
    if (connected) {
        // If we're here, the client was almost certainly disconnected.
        // But we check, just to make sure.
        if (client.is_connected()) {
            NES_INFO("MQTTSource: Shutting down and disconnecting from the MQTT server.");
            client.unsubscribe(topic)->wait();
            client.stop_consuming();
            client.disconnect()->wait();
            NES_INFO("MQTTSource: disconnected.");
        } else {
            NES_INFO("MQTTSource: Client was already disconnected");
        }
        connected = client.is_connected();
    }
    if (!connected) {
        NES_DEBUG("MQTTSource  " << this << ": disconnected");
    } else {
        NES_DEBUG("MQTTSource  " << this << ": NOT disconnected");
        return connected;
    }
    return !connected;
}
const string& MQTTSource::getServerAddress() const { return serverAddress; }
const string& MQTTSource::getClientId() const { return clientId; }
const string& MQTTSource::getUser() const { return user; }
const string& MQTTSource::getTopic() const { return topic; }
SourceType MQTTSource::getType() const { return MQTT_SOURCE; }

}// namespace NES
#endif
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

#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/MQTTSource.hpp>
#include <Util/Logger.hpp>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <mqtt/async_client.h>
#include <sstream>
#include <string>
#include <utility>

using namespace std;
using namespace std::chrono;

namespace NES {

MQTTSource::MQTTSource(SchemaPtr schema,
                       Runtime::BufferManagerPtr bufferManager,
                       Runtime::QueryManagerPtr queryManager,
                       const std::string& serverAddress,
                       const std::string& clientId,
                       const std::string& user,
                       const std::string& topic,
                       OperatorId operatorId,
                       OperatorId logicalSourceOperatorId,
                       size_t numSourceLocalBuffers,
                       GatheringMode gatheringMode,
                       std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors)
    : DataSource(std::move(schema),
                 std::move(bufferManager),
                 std::move(queryManager), operatorId, logicalSourceOperatorId, numSourceLocalBuffers,
                 gatheringMode,
                 std::move(executableSuccessors)),
      connected(false), serverAddress(serverAddress), clientId(clientId), user(user), topic(topic) {
    NES_DEBUG("MQTTSource  " << this << ": Init MQTTSource to " << serverAddress << " with client id: " << clientId << " and ");
    client = std::make_shared<mqtt::async_client>(serverAddress, clientId);
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

std::optional<Runtime::TupleBuffer> MQTTSource::receiveData() {
    NES_DEBUG("MQTTSource  " << this << ": receiveData ");

    if (connect()) {// was if (connect()) {
        try {
            NES_DEBUG("Waiting for messages on topic: '" << topic << "'");

            time_t timeout = 10;// 10 seconds time out
            time_t start = time(nullptr);
            time_t end = start + timeout;
            auto buffer = bufferManager->getBufferBlocking();

            while (start < end) {
                //ToDo: handle what happens if no data arrives
                start = time(nullptr);
                auto msg = client->consume_message();
                NES_DEBUG("Client consume message: '" << msg->get_payload_str() << "'");
                fillBuffer(buffer, msg->get_payload_str());

                return buffer;
            }
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
    return std::nullopt;
}

std::string MQTTSource::toString() const {
    std::stringstream ss;
    ss << "MQTTSOURCE(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    ss << "SERVERADDRESS=" << serverAddress << ", ";
    ss << "CLIENTID=" << clientId << ", ";
    ss << "USER=" << user << ", ";
    ss << "TOPIC=" << topic << ". ";
    return ss.str();
}

void MQTTSource::fillBuffer(Runtime::TupleBuffer& buf, const std::string& data) {

    NES_DEBUG("Client consume message: '" << data << "'");

    std::string line;
    std::vector<PhysicalTypePtr> physicalTypes;
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(schema->fields[0]->getDataType());

    uint64_t fieldSize = physicalField->size();

    if (physicalField->isBasicType()) {
        auto basicPhysicalField = std::dynamic_pointer_cast<BasicPhysicalType>(physicalField);
        /*
             * TODO: our types need their own sto/strto methods
             */
        if (basicPhysicalField->nativeType == BasicPhysicalType::UINT_64) {
            uint64_t val = std::stoull(data);
            memcpy(buf.getBuffer(), &val, fieldSize);
        } else if (basicPhysicalField->nativeType == BasicPhysicalType::INT_64) {
            int64_t val = std::stoll(data);
            memcpy(buf.getBuffer(), &val, fieldSize);
        } else if (basicPhysicalField->nativeType == BasicPhysicalType::UINT_32) {
            uint32_t val = std::stoul(data);
            memcpy(buf.getBuffer(), &val, fieldSize);
        } else if (basicPhysicalField->nativeType == BasicPhysicalType::INT_32) {
            int32_t val = std::stol(data);
            memcpy(buf.getBuffer(), &val, fieldSize);
        } else if (basicPhysicalField->nativeType == BasicPhysicalType::UINT_16) {
            uint16_t val = std::stol(data);
            memcpy(buf.getBuffer(), &val, fieldSize);
        } else if (basicPhysicalField->nativeType == BasicPhysicalType::INT_16) {
            int16_t val = std::stol(data);
            memcpy(buf.getBuffer(), &val, fieldSize);
        } else if (basicPhysicalField->nativeType == BasicPhysicalType::UINT_16) {
            uint8_t val = std::stoi(data);
            memcpy(buf.getBuffer(), &val, fieldSize);
        } else if (basicPhysicalField->nativeType == BasicPhysicalType::INT_8) {
            int8_t val = std::stoi(data);
            memcpy(buf.getBuffer(), &val, fieldSize);
        } else if (basicPhysicalField->nativeType == BasicPhysicalType::UINT_8) {
            int8_t val = std::stoi(data);
            memcpy(buf.getBuffer(), &val, fieldSize);
        } else if (basicPhysicalField->nativeType == BasicPhysicalType::DOUBLE) {
            double val = std::stod(data);
            memcpy(buf.getBuffer(), &val, fieldSize);
        } else if (basicPhysicalField->nativeType == BasicPhysicalType::FLOAT) {
            float val = std::stof(data);
            memcpy(buf.getBuffer(), &val, fieldSize);
        } else if (basicPhysicalField->nativeType == BasicPhysicalType::BOOLEAN) {
            bool val = (strcasecmp(data.c_str(), "true") == 0 || atoi(data.c_str()) != 0);
            memcpy(buf.getBuffer(), &val, fieldSize);
        }
    } else {
        memcpy(buf.getBuffer(), data.c_str(), fieldSize);
    }

    buf.setNumberOfTuples(1);
}

bool MQTTSource::connect() {
    if (!connected) {
        NES_DEBUG("MQTTSource was !connect now connect " << this << ": connected");
        // connect with user name and password
        try {
            auto connOpts = mqtt::connect_options_builder()
                                .user_name(user)
                                .clean_session(true)
                                .connect_timeout(std::chrono::nanoseconds(30 * 500 * 1000000))//15 seconds
                                .finalize();

            // Start consumer before connecting to make sure to not miss messages
            client->start_consuming();

            // Connect to the server
            NES_DEBUG("MQTTSource: Connecting to the MQTT server...");
            auto tok = client->connect(connOpts);

            // Getting the connect response will block waiting for the
            // connection to complete.
            auto rsp = tok->get_connect_response();

            // If there is no session present, then we need to subscribe, but if
            // there is a session, then the server remembers us and our
            // subscriptions.
            if (!rsp.is_session_present()) {
                client->subscribe(topic, 1)->wait();
            }
            connected = client->is_connected();
        } catch (const mqtt::exception& exc) {
            NES_WARNING("\n  " << exc);
            connected = false;
            return connected;
        }

        if (connected) {
            NES_DEBUG("MQTTSource: Connection established with topic: " << topic);
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
        if (client->is_connected()) {
            NES_DEBUG("MQTTSource: Shutting down and disconnecting from the MQTT server.");
            client->unsubscribe(topic)->wait();
            client->start_consuming();
            client->disconnect()->wait();
            NES_DEBUG("MQTTSource: disconnected.");
        } else {
            NES_DEBUG("MQTTSource: Client was already disconnected");
        }
        connected = client->is_connected();
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
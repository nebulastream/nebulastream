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
#include <Util/UtilityFunctions.hpp>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <mqtt/async_client.h>
#include <sstream>
#include <string>
#include <utility>
#include <stdlib.h>

using namespace std;
using namespace std::chrono;

namespace NES {
/*
 the user can specify the time unit for the delay and the duration of the delay in that time unit
 in order to avoid type switching types (different time units require different duration types), the user input for
 the duration is treated as nanoseconds and then multiplied to 'convert' to milliseconds or seconds accordingly
*/

MQTTSource::MQTTSource(SchemaPtr schema,
                       Runtime::BufferManagerPtr bufferManager,
                       Runtime::QueryManagerPtr queryManager,
                       std::string const& serverAddress,
                       std::string const& clientId,
                       std::string const& user,
                       std::string const& topic,
                       OperatorId operatorId,
                       size_t numSourceLocalBuffers,
                       GatheringMode gatheringMode,
                       std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors,
                       MQTTSourceDescriptor::DataType dataType)
    : DataSource(std::move(schema),
                 std::move(bufferManager),
                 std::move(queryManager),
                 operatorId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 std::move(executableSuccessors)),
      connected(false), serverAddress(serverAddress), clientId(clientId), user(user), topic(topic),
      dataType(dataType),
      tupleSize(schema->getSchemaSizeInBytes()){

    uint64_t i = random();
    client = std::make_shared<mqtt::async_client>(serverAddress, clientId + std::to_string(i));

    //fill buffer maximally
    tuplesThisPass = bufferManager->getBufferBlocking().getBufferSize() / tupleSize;

    //init physical types
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (const auto& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }

    NES_DEBUG("MQTTSource  " << this << ": Init MQTTSource to " << serverAddress << " with client id: " << clientId << ".");
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

    auto buffer = bufferManager->getBufferBlocking();
    if (connect()) {
            fillBuffer(buffer);
    } else {
        NES_ERROR("MQTTSource: Not connected!");
        return std::nullopt;
    }
    if (buffer.getNumberOfTuples() == 0){
        return std::nullopt;
    }
    return buffer;
}

std::string MQTTSource::toString() const {
    std::stringstream ss;
    ss << "MQTTSOURCE(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    ss << "SERVERADDRESS=" << serverAddress << ", ";
    ss << "CLIENTID=" << clientId << ", ";
    ss << "USER=" << user << ", ";
    ss << "TOPIC=" << topic << ", ";
    ss << "DATATYPE=" << dataType << ". ";
    return ss.str();
}

void MQTTSource::fillBuffer(Runtime::TupleBuffer& buf) {

    NES_DEBUG("MQTTSource::fillBuffer: fill buffer with #tuples=" << tuplesThisPass << " of size=" << tupleSize);

    uint64_t tupCnt = 0;
    while (tupCnt < tuplesThisPass) {
        std::string data = "";
        try {
            NES_DEBUG("Waiting for messages on topic: '" << topic << "'");
            auto msg = client->consume_message();
            NES_DEBUG("Client consume message: '" << msg->get_payload_str() << "'");
            data = msg->get_payload_str();
        } catch (const mqtt::exception& exc) {
            NES_ERROR("MQTTSource error: " << exc.what());
        } catch (...) {
            NES_ERROR("MQTTSource general error");
        }
        //init offset
        uint64_t offset = 0;
        NES_TRACE("MQTTSource line=" << tupCnt << " val=" << data);
        //init tokens
        std::vector<std::string> tokens;
        if (dataType == MQTTSourceDescriptor::JSON) {
            std::vector<std::string> helperToken;
            helperToken = UtilityFunctions::splitWithStringDelimiter(data, ":");
            std::string value;
            for (int i = 1; i < (int) helperToken.size(); i++) {
                value = UtilityFunctions::splitWithStringDelimiter(helperToken[i],",")[0];
                if (i == (int) helperToken.size() - 1){
                    value = value.substr(0, value.size() - 1);
                }
                tokens.push_back(value);
            }
        }

        for (uint64_t j = 0; j < schema->getSize(); j++) {
            auto field = physicalTypes[j];
            uint64_t fieldSize = field->size();

            NES_ASSERT2_FMT(fieldSize + offset + tupCnt * tupleSize < buf.getBufferSize(),
                            "Overflow detected: buffer size = " << buf.getBufferSize() << " position = "
                                                                << (offset + tupCnt * tupleSize) << " field size " << fieldSize);
            //ToDO change according to new memory layout
            if (field->isBasicType()) {
                NES_ASSERT2_FMT(!tokens[j].empty(), "Field cannot be empty if basic type");
                auto basicPhysicalField = std::dynamic_pointer_cast<BasicPhysicalType>(field);

                if (basicPhysicalField->nativeType == BasicPhysicalType::UINT_64) {
                    uint64_t val = std::stoull(tokens[j]);
                    memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->nativeType == BasicPhysicalType::INT_64) {
                    int64_t val = std::stoll(tokens[j]);
                    memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->nativeType == BasicPhysicalType::UINT_32) {
                    uint32_t val = static_cast<uint32_t>(std::stoul(tokens[j]));
                    memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->nativeType == BasicPhysicalType::INT_32) {
                    int32_t val = static_cast<int32_t>(std::stol(tokens[j]));
                    memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->nativeType == BasicPhysicalType::UINT_16) {
                    uint16_t val = static_cast<uint16_t>(std::stol(tokens[j]));
                    memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->nativeType == BasicPhysicalType::INT_16) {
                    int16_t val = static_cast<int16_t>(std::stol(tokens[j]));
                    memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->nativeType == BasicPhysicalType::UINT_8) {
                    uint8_t val = static_cast<uint8_t>(std::stoi(tokens[j]));
                    memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->nativeType == BasicPhysicalType::INT_8) {
                    int8_t val = static_cast<int8_t>(std::stoi(tokens[j]));
                    memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->nativeType == BasicPhysicalType::DOUBLE) {
                    double val = std::stod(tokens[j]);
                    memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->nativeType == BasicPhysicalType::FLOAT) {
                    float val = std::stof(tokens[j]);
                    memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->nativeType == BasicPhysicalType::BOOLEAN) {
                    bool val = (strcasecmp(tokens[j].c_str(), "true") == 0 || atoi(tokens[j].c_str()) != 0);
                    memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                } else if (basicPhysicalField->nativeType == BasicPhysicalType::CHAR) {
                    std::string val;
                    if (getDataType() == MQTTSourceDescriptor::JSON){
                        val = tokens[j].substr(2, tokens[j].size() - 2);
                    }
                    memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, val.c_str(), fieldSize);
                }
            } else {
                std::string val;
                if (dataType == MQTTSourceDescriptor::JSON){
                    val = tokens[j].substr(1, tokens[j].size() - 2);
                }
                NES_DEBUG("This value is: " << val);
                memcpy(buf.getBuffer<char>() + offset + tupCnt * tupleSize, val.c_str(), fieldSize);
            }

            offset += fieldSize;
        }
        tupCnt++;
    }//end of while
    buf.setNumberOfTuples(tupCnt);
    generatedTuples += tupCnt;
    generatedBuffers++;
}

bool MQTTSource::connect() {
    if (!connected) {
        NES_DEBUG("MQTTSource was !connect now connect " << this << ": connected");
        // connect with user name and password
        try {
            auto connOpts = mqtt::connect_options_builder()
                                .user_name(user)
                                .clean_session(true)
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
MQTTSourceDescriptor::DataType MQTTSource::getDataType() const { return dataType; }
uint64_t MQTTSource::getTupleSize() const { return tupleSize; }
SourceType MQTTSource::getType() const { return MQTT_SOURCE; }

}// namespace NES
#endif
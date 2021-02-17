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
#include <Sinks/Mediums/MQTTSink.hpp>

#include <cstdint>
#include <memory>
#include <sstream>
#include <string>

#include <NodeEngine/QueryManager.hpp>
#include <Util/Logger.hpp>


namespace NES {
/*
 the user can specify the time unit for the delay and the duration of the delay in that time unit
 in order to avoid type switching types (different time units require different duration types), the user input for
 the duration is treated as nanoseconds and then multiplied to 'convert' to milliseconds or seconds accordingly
*/
const uint32_t nanoToMillisecondsMultiplier = 1000000;
const uint32_t nanoToSecondsMultiplier = 1000000000;

SinkMediumTypes MQTTSink::getSinkMediumType() { return MQTT_SINK; }

MQTTSink::MQTTSink(SinkFormatPtr sinkFormat, QuerySubPlanId parentPlanId, const std::string& address,
                   const std::string& clientId, const std::string& topic, const std::string& user,
                   const uint32_t maxBufferedMSGs, const char timeUnit, const uint64_t msgDelay, bool asynchronousClient)
    : SinkMedium(sinkFormat, parentPlanId), address(address), clientId(clientId),topic(topic), user(user),
      maxBufferedMSGs(maxBufferedMSGs), timeUnit(timeUnit), msgDelay(msgDelay), asynchronousClient(asynchronousClient),
      qualityOfService(1),
      connected(false), sendDuration(std::chrono::nanoseconds(msgDelay * ((timeUnit=='m') ? nanoToMillisecondsMultiplier :
                                                                  (nanoToSecondsMultiplier * (timeUnit!='n') | (timeUnit=='n'))))),
      client(asynchronousClient, address, clientId, maxBufferedMSGs) {

    NES_DEBUG("MQTT Sink  " << this << ": Init MQTT Sink to " << address);
}

MQTTSink::~MQTTSink() {
    NES_DEBUG("MQTTSink::~MQTTSink: destructor called");
    bool success = disconnect();
    if (success) {
        NES_DEBUG("MQTTSink  " << this << ": Destroy MQTT Sink");
    } else {
        NES_ERROR("MQTTSink  " << this << ": Destroy MQTT Sink failed cause it could not be disconnected");
        throw Exception("MQTT Sink destruction failed");
    }
    NES_DEBUG("MQTTSink  " << this << ": Destroy MQTT Sink");
}


bool MQTTSink::writeData(NodeEngine::TupleBuffer& inputBuffer, NodeEngine::WorkerContextRef) {
    try {
        std::unique_lock lock(writeMutex);
        if (!connected) {
            NES_DEBUG("ZmqSink  " << this << ": cannot write buffer " << inputBuffer << " because queue is not connected");
            throw Exception("Write to zmq sink failed");
        }
        if (!inputBuffer.isValid()) {
            NES_ERROR("ZmqSink::writeData input buffer invalid");
            return false;
        }

        if (asynchronousClient) {
            mqtt::topic top(*client.getAsyncClient(), topic, qualityOfService, true);
            for (uint32_t j = 0; j < inputBuffer.getNumberOfTuples() * 2; j = j + 2) {
                std::string value = std::to_string(inputBuffer.getBuffer<uint32_t>()[j + 1]);
                std::string payload = "{\"temperature\":" + value + "}";
                top.publish(std::move(payload));
                std::this_thread::sleep_for(sendDuration);
            }
        } else {
            MQTTClientWrapper::UserCallback cb;
            client.setCallback(cb);
            for (uint32_t j = 0; j < inputBuffer.getNumberOfTuples() * 2; j = j + 2) {
                std::string value = std::to_string(inputBuffer.getBuffer<uint32_t>()[j + 1]);
                std::string payload = "{\"temperature\":" + value + "}";
                auto pubmsg = mqtt::make_message(topic, payload);
                pubmsg->set_qos(qualityOfService);
                (*client.getSyncClient()).publish(pubmsg);
                std::this_thread::sleep_for(sendDuration);
            }
            NES_DEBUG("Synchronous Case");
        }
        if(asynchronousClient && client.getNumberOfUnsentMessages() > 0) {
            NES_ERROR("Unsent messages");
        }
    }
    catch (const mqtt::exception& ex) {
        NES_ERROR("Error during writeData in MQTT sink: " << ex.what());
        return false;
    }
    return true;
}


const std::string MQTTSink::toString() const {
    std::stringstream ss;
    ss << "MQTT_SINK(";
    ss << "SCHEMA("    << sinkFormat->getSchemaPtr()->toString() << "), ";
    ss << "ADDRESS"    << address << ", ";
    ss << "CLIENT_ID=" << clientId << ", ";
    ss << "TOPIC="     << topic    << ", ";
    ss << "USER="      << user     << ", ";
    ss << "MAX_BUFFERED_MESSAGES=" << maxBufferedMSGs << ", ";
    ss << "TIME_UNIT=" << timeUnit << ", ";
    ss << "SEND_PERIOD=" << msgDelay << ", ";
    ss << "SEND_DURATION_IN_NS=" << std::to_string(sendDuration.count()) << ", ";
    ss << "QUALITY_OF_SERVICE=" << std::to_string(qualityOfService) << ", ";
    ss << "CLIENT_TYPE=" << ((asynchronousClient) ? "ASYMMETRIC_CLIENT" : "SYMMETRIC_CLIENT");
    ss << ")";
    return ss.str();
}

bool MQTTSink::connect() {
    if (!connected) {
        try {
            auto connOpts = mqtt::connect_options_builder()
                .keep_alive_interval(maxBufferedMSGs * sendDuration)
                .user_name(user)
                .clean_session(true)
                .automatic_reconnect(true)
                .finalize();

            // Connect to the MQTT broker
            NES_DEBUG("MQTTSink: connect to address=" << address);
            client.connect(connOpts);
            connected = true;
        } catch (const mqtt::exception& ex) {
            NES_ERROR("MQTTSink: " << ex.what());
        }
    }
    if (connected) {
        NES_DEBUG("MQTTSink  " << this << ": connected address=" << address);
    } else {
        NES_DEBUG("MQTTSink  " << this << ": NOT connected=" << address);
    }
    return connected;
}

bool MQTTSink::disconnect() {
    if (connected) {
        client.disconnect();
        connected = false;
    }
    if (!connected) {
        NES_DEBUG("MQTTSink  " << this << ": disconnected");
    } else {
        NES_DEBUG("MQTTSink  " << this << ": NOT disconnected");
    }
    return !connected;
}

bool MQTTSink::getConnected() { return connected; }
const std::string& MQTTSink::getAddress() const { return address; }
const std::string& MQTTSink::getClientId() const { return clientId; }
const std::string& MQTTSink::getTopic() const { return topic; }
const std::string& MQTTSink::getUser() const { return user; }
const uint32_t MQTTSink::getMaxBufferedMSGs() const { return maxBufferedMSGs; }
const char MQTTSink::getTimeUnit() const { return timeUnit; }
const uint64_t MQTTSink::getMsgDelay() const { return msgDelay; }
const bool MQTTSink::getAsynchronousClient() const { return asynchronousClient; }

}// namespace NES

#endif

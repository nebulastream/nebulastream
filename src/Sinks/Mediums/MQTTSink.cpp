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
const uint32_t NANO_TO_MILLI_SECONDS_MULTIPLIER = 1000000;
const uint32_t NANO_TO_SECONDS_MULTIPLIER = 1000000000;

SinkMediumTypes MQTTSink::getSinkMediumType() { return MQTT_SINK; }

MQTTSink::MQTTSink(SinkFormatPtr sinkFormat, QuerySubPlanId parentPlanId, const std::string address,
                   const std::string clientId, const std::string topic, const std::string user,
                   uint64_t maxBufferedMSGs, const TimeUnits timeUnit, uint64_t msgDelay,
                   const ServiceQualities qualityOfService, bool asynchronousClient) :
      SinkMedium(sinkFormat, parentPlanId), address(address), clientId(clientId),topic(topic), user(user),
      maxBufferedMSGs(maxBufferedMSGs), timeUnit(timeUnit), msgDelay(msgDelay), qualityOfService(qualityOfService),
      asynchronousClient(asynchronousClient), connected(false)
{
    minDelayBetweenSends = std::chrono::nanoseconds(msgDelay * ((timeUnit==milliseconds) ? NANO_TO_MILLI_SECONDS_MULTIPLIER
                                                :(NANO_TO_SECONDS_MULTIPLIER * (timeUnit!=nanoseconds) | (timeUnit==nanoseconds))));
    std::cout << "DELAY: " << minDelayBetweenSends.count();
    client = std::make_shared<MQTTClientWrapper>(asynchronousClient, address, clientId, maxBufferedMSGs);
    NES_DEBUG("MQTTSink::~MQTTSink " << this->toString() << ": Init MQTT Sink to " << address);
}

MQTTSink::~MQTTSink() {
    NES_DEBUG("MQTTSink::~MQTTSink: destructor called");
    bool success = disconnect();
    if (success) {
        NES_DEBUG("MQTTSink::~MQTTSink " << this << ": MQTT Sink Destroyed");
    } else {
        NES_ERROR("MQTTSink::~MQTTSink " << this << ": Destroy MQTT Sink failed cause it could not be disconnected");
        throw Exception("MQTT Sink destruction failed");
    }
}


bool MQTTSink::writeData(NodeEngine::TupleBuffer& inputBuffer, NodeEngine::WorkerContextRef) {
    try {
        std::unique_lock lock(writeMutex);
        if (!connected) {
            NES_DEBUG("MQTTSink::writeData  " << this << ": cannot write buffer " << inputBuffer << " because queue is not connected");
            throw Exception("Write to zmq sink failed");
        }
        if (!inputBuffer.isValid()) {
            NES_ERROR("MQTTSink::writeData input buffer invalid");
            return false;
        }

        if (asynchronousClient) {
            mqtt::topic sendTopic(*client->getAsyncClient(), topic, qualityOfService, true);
            /* Iterate over the input TupleBuffer. In this case each Tuple of the TupleBuffer contains 64 bit (uint64).
               32 bit are used for the key (uint32) and the remaining 32 bit for the value (also uint32).
               For now we are only interested in the value, so we grab the value at j+1 and increase j+2 each iteration. */
            for (uint32_t j = 0; j < inputBuffer.getNumberOfTuples() * 2; j = j + 2) {
                std::string value = std::to_string(inputBuffer.getBuffer<uint32_t>()[j + 1]);
                std::string payload = "{\"temperature\":" + value + "}";
                sendTopic.publish(std::move(payload));
                std::this_thread::sleep_for(minDelayBetweenSends);
            }
        } else {
            MQTTClientWrapper::UserCallback cb;
            client->setCallback(cb);
            // look at the asynchronousClient case for information about the TupleBuffer usage
            for (uint32_t j = 0; j < inputBuffer.getNumberOfTuples() * 2; j = j + 2) {
                std::string value = std::to_string(inputBuffer.getBuffer<uint32_t>()[j + 1]);
                std::string payload = "{\"temperature\":" + value + "}";
                // synchronous clients do not allow for sending via mqtt::topic. Instead, each message is constructed
                // individually. The message is given a particular quality of service and then sent synchronously.
                auto pubmsg = mqtt::make_message(topic, payload);
                pubmsg->set_qos(qualityOfService);
                (*client->getSyncClient()).publish(pubmsg);
                std::this_thread::sleep_for(minDelayBetweenSends);
            }
        }
        // If the connection to the MQTT broker is lost during sending, the MQTT client might buffer all the remaining messages.
        if(asynchronousClient && client->getNumberOfUnsentMessages() > 0) {
            NES_ERROR("MQTTSink::writeData: " << client->getNumberOfUnsentMessages() << " messages could not be sent");
        }
    }
    catch (const mqtt::exception& ex) {
        NES_ERROR("MQTTSink::writeData: Error during writeData in MQTT sink: " << ex.what());
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
    ss << "SEND_DURATION_IN_NS=" << std::to_string(minDelayBetweenSends.count()) << ", ";
    ss << "QUALITY_OF_SERVICE=" << std::to_string(qualityOfService) << ", ";
    ss << "CLIENT_TYPE=" << ((asynchronousClient) ? "ASYMMETRIC_CLIENT" : "SYMMETRIC_CLIENT");
    ss << ")";
    return ss.str();
}

bool MQTTSink::connect() {
    if (!connected) {
        try {
            auto connOpts = mqtt::connect_options_builder()
                .keep_alive_interval(maxBufferedMSGs * minDelayBetweenSends)
                .user_name(user)
                .clean_session(true)
                .automatic_reconnect(true)
                .finalize();
            // Connect to the MQTT broker
            NES_DEBUG("MQTTSink::connect: connect to address=" << address);
            client->connect(connOpts);
            connected = true;
        } catch (const mqtt::exception& ex) {
            NES_ERROR("MQTTSink::connect:  " << ex.what());
        }
    }
    if (connected) {
        NES_DEBUG("MQTTSink::disconnect: " << this << ": connected address=" << address);
    } else {
        NES_DEBUG("MQTTSink::disconnect: " << this << ": NOT connected=" << address);
    }
    return connected;
}

bool MQTTSink::disconnect() {
    if (connected) {
        client->disconnect();
        connected = false;
    }
    if (!connected) {
        NES_DEBUG("MQTTSink::disconnect: " << this << ": disconnected");
    } else {
        NES_DEBUG("MQTTSink::disconnect: " << this << ": NOT disconnected");
    }
    NES_TRACE("MQTTSink::disconnect: connected value is" << connected);
    return !connected;
}

const std::string MQTTSink::getAddress() const { return address; }
const std::string MQTTSink::getClientId() const { return clientId; }
const std::string MQTTSink::getTopic() const { return topic; }
const std::string MQTTSink::getUser() const { return user; }
uint64_t MQTTSink::getMaxBufferedMSGs() { return maxBufferedMSGs; }
const MQTTSink::TimeUnits MQTTSink::getTimeUnit() const { return timeUnit; }
uint64_t MQTTSink::getMsgDelay() { return msgDelay; }
const MQTTSink::ServiceQualities MQTTSink::getQualityOfService() const { return qualityOfService; }
bool MQTTSink::getAsynchronousClient() { return asynchronousClient; }

}// namespace NES

#endif

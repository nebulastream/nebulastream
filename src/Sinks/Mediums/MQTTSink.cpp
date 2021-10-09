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
#include <Runtime/QueryManager.hpp>
#include <Sinks/Mediums/MQTTSink.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

namespace NES {
/*
 the user can specify the time unit for the delay and the duration of the delay in that time unit
 in order to avoid type switching types (different time units require different duration types), the user input for
 the duration is treated as nanoseconds and then multiplied to 'convert' to milliseconds or seconds accordingly
*/
const uint32_t NANO_TO_MILLI_SECONDS_MULTIPLIER = 1000000;
const uint32_t NANO_TO_SECONDS_MULTIPLIER = 1000000000;

SinkMediumTypes MQTTSink::getSinkMediumType() { return MQTT_SINK; }

MQTTSink::MQTTSink(SinkFormatPtr sinkFormat,
                   QuerySubPlanId parentPlanId,
                   const std::string& address,
                   const std::string& clientId,
                   const std::string& topic,
                   const std::string& user,
                   uint64_t maxBufferedMSGs,
                   MQTTSinkDescriptor::TimeUnits timeUnit,
                   uint64_t messageDelay,
                   MQTTSinkDescriptor::ServiceQualities qualityOfService,
                   bool asynchronousClient)
    : SinkMedium(std::move(sinkFormat), parentPlanId), address(address), clientId(clientId), topic(topic), user(user),
      maxBufferedMSGs(maxBufferedMSGs), timeUnit(timeUnit), messageDelay(messageDelay), qualityOfService(qualityOfService),
      asynchronousClient(asynchronousClient), connected(false) {

    minDelayBetweenSends =
        std::chrono::nanoseconds(messageDelay
                                 * ((timeUnit == MQTTSinkDescriptor::TimeUnits::milliseconds)
                                        ? NANO_TO_MILLI_SECONDS_MULTIPLIER
                                        : (NANO_TO_SECONDS_MULTIPLIER * (timeUnit != MQTTSinkDescriptor::TimeUnits::nanoseconds)
                                           | (timeUnit == MQTTSinkDescriptor::TimeUnits::nanoseconds))));
    std::cout << "DELAY: " << minDelayBetweenSends.count();

    client = std::make_shared<MQTTClientWrapper>(asynchronousClient, address, clientId, maxBufferedMSGs, topic, qualityOfService);
    NES_TRACE("MQTTSink::MQTTSink " << this->toString() << ": Init MQTT Sink to " << address);
}

MQTTSink::~MQTTSink() NES_NOEXCEPT(false) {
    NES_TRACE("MQTTSink::~MQTTSink: destructor called");
    bool success = disconnect();
    if (success) {
        NES_TRACE("MQTTSink::~MQTTSink " << this << ": MQTT Sink Destroyed");
    } else {
        NES_ASSERT2_FMT(false, "MQTTSink::~MQTTSink " << this << ": Destroy MQTT Sink failed cause it could not be disconnected");
    }
}

bool MQTTSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) {
    std::unique_lock lock(writeMutex);
    NES_ASSERT(connected, "MQTTSink::writeData: cannot write buffer because client is not connected");

    if (!inputBuffer.isValid()) {
        NES_ERROR("MQTTSink::writeData input buffer invalid");
        return false;
    }
    // Print received Tuple Buffer for debugging purposes.
    NES_TRACE("MQTTSink::writeData" << Util::prettyPrintTupleBuffer(inputBuffer, sinkFormat->getSchemaPtr()));

    try {
        // Main share work performed here. The input TupleBuffer is iterated over and each tuple is converted to a json string
        // and afterwards sent to an MQTT broker, via the MQTT client
        for (auto formattedTuple : sinkFormat->getTupleIterator(inputBuffer)) {
            if(formattedTuple == "") {
                NES_TRACE("MQTTSink:: Error during tuple creation from tuple buffer: " << formattedTuple);
                continue;
            }
            NES_TRACE("MQTTSink::writeData Sending Payload: " << formattedTuple);
            client->sendPayload(formattedTuple);
            std::this_thread::sleep_for(minDelayBetweenSends);
        }

        // When the client is asynchronous it can happen that the client's buffer is large enough to buffer all messages
        // that were not successfully sent to an MQTT broker.
        if ((asynchronousClient && client->getNumberOfUnsentMessages() > 0)) {
            NES_ERROR("MQTTSink::writeData: " << client->getNumberOfUnsentMessages() << " messages could not be sent");
            return false;
        }
    } catch (const mqtt::exception& ex) {
        NES_ERROR("MQTTSink::writeData: Error during writeData in MQTT sink: " << ex.what());
        return false;
    }
    return true;
}

std::string MQTTSink::toString() const {
    std::stringstream ss;
    ss << "MQTT_SINK(";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << "), ";
    ss << "ADDRESS=" << address << ", ";
    ss << "CLIENT_ID=" << clientId << ", ";
    ss << "TOPIC=" << topic << ", ";
    ss << "USER=" << user << ", ";
    ss << "MAX_BUFFERED_MESSAGES=" << maxBufferedMSGs << ", ";
    ss << "TIME_UNIT=" << timeUnit << ", ";
    ss << "SEND_PERIOD=" << messageDelay << ", ";
    ss << "SEND_DURATION_IN_NS=" << std::to_string(minDelayBetweenSends.count()) << ", ";
    ss << "QUALITY_OF_SERVICE=" << std::to_string(qualityOfService) << ", ";
    ss << "CLIENT_TYPE=" << ((asynchronousClient) ? "ASYMMETRIC_CLIENT" : "SYMMETRIC_CLIENT");
    ss << ")";
    return ss.str();
}

bool MQTTSink::connect() {
    std::unique_lock lock(writeMutex);
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
    std::unique_lock lock(writeMutex);
    if (connected) {
        client->disconnect();
        connected = false;
    } else {
        NES_DEBUG("MQTTSink::disconnect: " << this << ": NOT connected");
    }
    NES_TRACE("MQTTSink::disconnect: connected value is" << connected);
    return !connected;
}

std::string MQTTSink::getAddress() const { return address; }
std::string MQTTSink::getClientId() const { return clientId; }
std::string MQTTSink::getTopic() const { return topic; }
std::string MQTTSink::getUser() const { return user; }
uint64_t MQTTSink::getMaxBufferedMSGs() const { return maxBufferedMSGs; }
MQTTSinkDescriptor::TimeUnits MQTTSink::getTimeUnit() const { return timeUnit; }
uint64_t MQTTSink::getMsgDelay() const { return messageDelay; }
MQTTSinkDescriptor::ServiceQualities MQTTSink::getQualityOfService() const { return qualityOfService; }
bool MQTTSink::getAsynchronousClient() const { return asynchronousClient; }
#endif
}// namespace NES

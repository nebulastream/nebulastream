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

#include <Sinks/Mediums/MQTTSink.hpp>

#include <cstdint>
#include <memory>
#include <sstream>
#include <string>

#include <NodeEngine/QueryManager.hpp>
#include <Util/Logger.hpp>

#include <Util/UtilityFunctions.hpp>

#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES {
/*
 the user can specify the time unit for the delay and the duration of the delay in that time unit
 in order to avoid type switching types (different time units require different duration types), the user input for
 the duration is treated as nanoseconds and then multiplied to 'convert' to milliseconds or seconds accordingly
*/
const uint32_t NANO_TO_MILLI_SECONDS_MULTIPLIER = 1000000;
const uint32_t NANO_TO_SECONDS_MULTIPLIER = 1000000000;

SinkMediumTypes MQTTSink::getSinkMediumType() { return MQTT_SINK; }

MQTTSink::MQTTSink(SinkFormatPtr sinkFormat, QuerySubPlanId parentPlanId, const std::string address, const std::string clientId,
                   const std::string topic, const std::string user, uint64_t maxBufferedMSGs, const TimeUnits timeUnit,
                   uint64_t messageDelay, const ServiceQualities qualityOfService, bool asynchronousClient)
    : SinkMedium(sinkFormat, parentPlanId), address(address), clientId(clientId), topic(topic), user(user),
      maxBufferedMSGs(maxBufferedMSGs), timeUnit(timeUnit), messageDelay(messageDelay), qualityOfService(qualityOfService),
      asynchronousClient(asynchronousClient), connected(false) {

    minDelayBetweenSends = std::chrono::nanoseconds(
        messageDelay
        * ((timeUnit == milliseconds) ? NANO_TO_MILLI_SECONDS_MULTIPLIER
                                      : (NANO_TO_SECONDS_MULTIPLIER * (timeUnit != nanoseconds) | (timeUnit == nanoseconds))));
    std::cout << "DELAY: " << minDelayBetweenSends.count();
    client = std::make_shared<MQTTClientWrapper>(asynchronousClient, address, clientId, maxBufferedMSGs, topic, qualityOfService);
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

bool MQTTSink::sendDataFromTupleBuffer(NodeEngine::TupleBuffer& inputBuffer, SchemaPtr schemaPtr) {
    std::vector<uint32_t> fieldOffsets;
    std::vector<PhysicalTypePtr> types;
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    // Iterate over all fields of a tuple. Store sizes in fieldOffsets, to calculate correct offsets in the step below.
    // Also, store types of fields in a separate array. Is later used to convert values to strings correctly.
    for (uint32_t i = 0; i < schemaPtr->getSize(); ++i) {
        auto physicalType = physicalDataTypeFactory.getPhysicalType(schemaPtr->get(i)->getDataType());
        fieldOffsets.push_back(physicalType->size());
        types.push_back(physicalType);
        NES_TRACE("CodeGenerator: " + std::string("Field Size ") + schemaPtr->get(i)->toString() + std::string(": ")
                      + std::to_string(physicalType->size()));
    }

    // Iteratively add up all the sizes in the offset array, to correctly determine where each field starts in the TupleBuffer
    // From fieldOffsets = {64, 32, 32, 128} (field sizes) to fieldOffsets = {64, 96, 128, 256} (actual field offsets)
    uint32_t prefix_sum = 0;
    for (uint32_t i = 0; i < fieldOffsets.size(); ++i) {
        uint32_t val = fieldOffsets[i];
        fieldOffsets[i] = prefix_sum;
        prefix_sum += val;
        NES_TRACE("CodeGenerator: " + std::string("Prefix SumAggregationDescriptor: ") + schemaPtr->get(i)->toString()
                      + std::string(": ") + std::to_string(fieldOffsets[i]));
    }

    /*
        Get pointers to all tuples in TupleBuffer.
        Iterate over tuples and by using the field offset array, access each field of a tuple in an iteration and
        convert the fields content to a string, that is added to the JSON message. The field name will be used as key.
     */
    std::string jsonMessage;
    std::vector<char*> tuplePointers = inputBuffer.getTuplesWithSchema(schemaPtr);
    if(tuplePointers.empty()) {
        NES_ERROR("MQTTSink::sendDataFromTupleBuffer: Tuple Buffer is empty. Can not extract any data");
        return false;
    }
    //iterate over all tuples in the TupleBuffer, convert each tuple to a JSON string
    for(auto tuplePointer : tuplePointers) {
        jsonMessage = "{";
        // Add first tuple to json object and if there is more, add rest.
        // Adding the first tuple before the loop avoids checking if last tuple is processed in order to omit "," after json value
        uint32_t currentField = 0;
        std::string fieldValue = types[currentField]->convertRawToString(&tuplePointer[fieldOffsets[currentField]]);
        jsonMessage += schemaPtr->fields[currentField]->getName() + ":" + fieldValue;
        // Iterate over all fields in a tuple. Get field offsets from fieldOffsets array. Use fieldNames as keys and TupleBuffer
        // values as the corresponding values
        for (++currentField; currentField < schemaPtr->fields.size(); ++currentField) {
            jsonMessage += ",";
            void* fieldPointer = &tuplePointer[fieldOffsets[currentField]];
            fieldValue = types[currentField]->convertRawToString(fieldPointer);
            jsonMessage += schemaPtr->fields[currentField]->getName() + ":" + fieldValue;
        }
        jsonMessage += "}";

        NES_TRACE("MQTTSink::writeData Sending Payload: " << jsonMessage);
        client->sendPayload(jsonMessage);
        std::this_thread::sleep_for(minDelayBetweenSends);
    }

    return true;
}


bool MQTTSink::writeData(NodeEngine::TupleBuffer& inputBuffer, NodeEngine::WorkerContextRef) {
    try {
        std::unique_lock lock(writeMutex);
        if (!connected) {
            NES_DEBUG("MQTTSink::writeData  " << this << ": cannot write buffer " << inputBuffer
                                              << " because queue is not connected");
            throw Exception("Write to zmq sink failed");
        }

        if (!inputBuffer.isValid()) {
            NES_ERROR("MQTTSink::writeData input buffer invalid");
            return false;
        }

        // Print received Tuple Buffer for debugging purposes.
        NES_TRACE("MQTTSink::writeData" << UtilityFunctions::prettyPrintTupleBuffer(inputBuffer, sinkFormat->getSchemaPtr()));

        // Right now the only supported sink format is the JSON_FORMAT. Also, the getData function right now is a stub.
        auto dataBuffers = sinkFormat->getData(inputBuffer);
        bool successfullySentData = false;
        for(auto dataBuffer : dataBuffers) {
            successfullySentData = sendDataFromTupleBuffer(inputBuffer, sinkFormat->getSchemaPtr());
        }
        // If the connection to the MQTT broker is lost during sending, the MQTT client might buffer all the remaining messages.
        if (!successfullySentData || (asynchronousClient && client->getNumberOfUnsentMessages() > 0)) {
            NES_ERROR("MQTTSink::writeData: " << client->getNumberOfUnsentMessages() << " messages could not be sent");
        }
    } catch (const mqtt::exception& ex) {
        NES_ERROR("MQTTSink::writeData: Error during writeData in MQTT sink: " << ex.what());
        return false;
    }
    return true;
}

const std::string MQTTSink::toString() const {
    std::stringstream ss;
    ss << "MQTT_SINK(";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << "), ";
    ss << "ADDRESS" << address << ", ";
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
uint64_t MQTTSink::getMsgDelay() { return messageDelay; }
const MQTTSink::ServiceQualities MQTTSink::getQualityOfService() const { return qualityOfService; }
bool MQTTSink::getAsynchronousClient() { return asynchronousClient; }

}// namespace NES

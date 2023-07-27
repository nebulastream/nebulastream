//
// Created by Kasper Hjort Berthelsen on 10/11/2022.
//

#include <API/AttributeField.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/LoRaWANProxySource.hpp>
#include <Sources/Parsers/JSONParser.hpp>

#include <Util/Common.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <nlohmann/json.hpp>
#include <utility>

// some utility function shamelessly taken from https://stackoverflow.com/a/34571089
static std::string base64_encode(const std::string& in) {

    std::string out;

    int val = 0, valb = -6;
    for (unsigned char c : in) {
        val = (val << 8) + c;
        valb += 8;
        while (valb >= 0) {
            out.push_back("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"[(val >> valb) & 0x3F]);
            valb -= 6;
        }
    }
    if (valb > -6)
        out.push_back("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"[((val << 8) >> (valb + 8)) & 0x3F]);
    while (out.size() % 4)
        out.push_back('=');
    return out;
}

static std::string base64_decode(const std::string& in) {

    std::string out;

    std::vector<int> T(256, -1);
    for (int i = 0; i < 64; i++)
        T["ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"[i]] = i;

    int val = 0, valb = -8;
    for (unsigned char c : in) {
        if (T[c] == -1)
            break;
        val = (val << 6) + T[c];
        valb += 6;
        if (valb >= 0) {
            out.push_back(char((val >> valb) & 0xFF));
            valb -= 8;
        }
    }
    return out;
}

namespace NES {

LoRaWANProxySource::NetworkServer::NetworkServer(const std::string& url,
                                                 const std::string& user,
                                                 const std::string& password,
                                                 const std::string& appId,
                                                 const std::vector<std::string>& deviceEUIs)
    : url(url), user(user), password(password), appId(appId), deviceEUIs(deviceEUIs) {}

std::string LoRaWANProxySource::NetworkServer::toString() {
    std::stringstream ss;
    ss << "NetworkServer => {\n";
    ss << "URL: " << url << "\n";
    ss << "user: " << user << "\n";
    ss << "appId: " << appId << "\n";
    ss << "deviceEUIS: [ ";
    for (const auto& deviceEui : deviceEUIs) {
        ss << deviceEui << ", ";
    }
    ss << " ]\n";
    ss << "}";
    return ss.str();
}

#pragma region chirpstack
LoRaWANProxySource::ChirpStackServer::ChirpStackServer(const std::string& url,
                                                       const std::string& user,
                                                       const std::string& password,
                                                       const std::string& appId,
                                                       const std::vector<std::string>& deviceEUIs)
    : NetworkServer(url, user, password, appId, deviceEUIs), client(url, "LoRaWANProxySource"), topicBase("application/" + appId),
      topicAll(topicBase + "/#"), topicDevice(topicBase + "/device"), topicReceiveSuffix("/event/up"),
      topicSendSuffix("/command/down"), topicAllDevicesReceive(topicDevice + "/+" + topicReceiveSuffix) {}

bool LoRaWANProxySource::ChirpStackServer::isConnected() { return client.is_connected(); }
bool LoRaWANProxySource::ChirpStackServer::connect() {
    if (!client.is_connected()) {
        //open();
        try {
            //TODO: actually get authentication to work
            //            auto sslopt = mqtt::ssl_options_builder()
            //                              .ca_path(capath)
            //                              .trust_store(certpath)
            //                              .private_key(keypath)
            //                              .verify(false)
            //                              .enable_server_cert_auth(false)
            //                              .finalize();
            //            //automatic reconnect = true enables establishing a connection with a broker again, after a disconnect
            auto connOpts = mqtt::connect_options_builder()
                                //.ssl(sslopt)
                                .user_name(user)
                                .password(password)
                                .automatic_reconnect(true)
                                .clean_session(true)
                                .finalize();

            // Start consumer before connecting to make sure to not miss messages
            client.start_consuming();

            // Connect to the server
            NES_DEBUG("LoRaWANProxySource::connect Connecting to the MQTT server...");
            auto rsp = client.connect(connOpts);

            // If there is no session present, then we need to subscribe, but if
            // there is a session, then the server remembers us and our
            // subscriptions.
            if (!rsp.is_session_present()) {
                auto resp = client.subscribe(topicAllDevicesReceive, 0);
                //TODO: handle errors
            }
        } catch (const mqtt::exception& error) {
            NES_ERROR("LoRaWANProxySource::connect: {}", error.what());
            return false;
        }
    }
    return true;
}
bool LoRaWANProxySource::ChirpStackServer::disconnect() {
    NES_DEBUG("LoRaWANProxySource::disconnect connected={}", client.is_connected());
    if (client.is_connected()) {
        //close();
        NES_DEBUG("LoRaWANProxySource: Shutting down and disconnecting from the MQTT server.");

        client.unsubscribe(topicAllDevicesReceive);
        //TODO: Handle Errors
        client.disconnect();
        NES_DEBUG("LoRaWANProxySource::disconnect: disconnected.");
    } else {
        NES_DEBUG("LoRaWANProxySource::disconnect: Client was already disconnected");
    }

    if (!client.is_connected()) {
        NES_DEBUG("LoRaWANProxySource::disconnect: {}: disconnected ", this->toString());
    } else {
        NES_DEBUG("LoRaWANProxySource::disconnect: {}: NOT disconnected", this->toString());
        return false;
    }
    return true;
}
EndDeviceProtocol::Output LoRaWANProxySource::ChirpStackServer::receiveData() {
    NES_DEBUG("LoRaWANProxySource {}: receiveData", this->toString());

    if (!isConnected()) {//connects if not connected, or return true if already connected. Might be bad design
        connect();
    }
    NES_TRACE("Connected and listening for mqtt messages for 1 sec")
    auto output = EndDeviceProtocol::Output();
    auto count = 0;
    while (true) {
        mqtt::const_message_ptr msg;
        auto msgReceived = client.try_consume_message_for(&msg, std::chrono::seconds(1));
        if (!msgReceived) {
            NES_TRACE("no messages received")
            break;//Exit if no new messages
        }
        ++count;
        auto rcvStr = msg->get_payload_str();
        auto rcvTopic = msg->get_topic();
        NES_TRACE("Received msg no. {} on topic: \"{}\" with payload: \"{}\"", count, rcvTopic, rcvStr)
        // 0                                55            78
        // |                                |             |
        // application/APPLICATION_ID/device/DEV_EUI/event/EVENT
        auto devEUI = rcvTopic.substr(56, 64 / 4);
        auto event = rcvTopic.substr(79, rcvTopic.length());

        //rcvStr should be formatted as shown here: https://www.chirpstack.io/docs/chirpstack/integrations/events.html#up---uplink-event
        //most important is data which is placed on the "data" field
        try {
            auto js = nlohmann::json::parse(rcvStr);
            if (!js.contains("data")) {
                NES_WARNING("LoRaWANProxySource: parsed json does not contain data field");
            } else {
                output.ParseFromString(base64_decode(js["data"]));
            }
        } catch (nlohmann::json::parse_error& ex) {
            NES_WARNING("LoRaWANProxySource: JSON parse error {}", ex.what());
        }
    }
    return output;
}

bool LoRaWANProxySource::ChirpStackServer::sendMessage(EndDeviceProtocol::Message message) {
    auto pbEncoded = base64_encode(message.SerializeAsString());

    for (const auto& devEUI : deviceEUIs) {
        //JSON payload must conform to:
        //        {
        //            "devEui": "0102030405060708",             // this must match the DEV_EUI of the MQTT topic
        //            "confirmed": true,                        // whether the payload must be sent as confirmed data down or not
        //            "fPort": 10,                              // FPort to use (must be > 0)
        //            "data": "...."                            // base64 encoded data (plaintext, will be encrypted by ChirpStack)
        //            "object": {                               // decoded object (when application coded has been configured)
        //                "temperatureSensor": {"1": 25},       // when providing the 'object', you can omit 'data'
        //                "humiditySensor": {"1": 32}
        //            }
        //        }
        nlohmann::json payload{{"devEui", devEUI}, {"confirmed", true}, {"fPort", 1}, {"data", pbEncoded}};
        auto topic = topicDevice + "/" + devEUI + topicSendSuffix;
        auto payload_dump = payload.dump();
        NES_DEBUG("sending data: {} to topic: {} with payload {}", message.DebugString(), topic, payload.dump());
        client.publish(mqtt::make_message(topic, payload_dump));
    }
    return true;//TODO: Handle errors
}
LoRaWANProxySource::ChirpStackServer::~ChirpStackServer() = default;
LoRaWANProxySource::ChirpStackServer::ChirpStackEvent LoRaWANProxySource::ChirpStackServer::strToEvent(const std::string& s) {
    return magic_enum::enum_cast<ChirpStackEvent>(s).value();
}

std::string LoRaWANProxySource::ChirpStackServer::toString() {
    std::stringstream ss;
    ss << "ChirpStackServer => {\n";
    ss << "NetworkServer: " << NetworkServer::toString() << "\n";
    ss << "MQTT client: " << client.get_client_id() << "\n";
    ss << "MQTT url: " << client.get_server_uri() << "\n";
    return ss.str();
}
#pragma endregion chirpstack

#pragma region ttnstack
LoRaWANProxySource::TheThingsNetworkServer::TheThingsNetworkServer(const std::string& url,
                                                                   const std::string& user,
                                                                   const std::string& password,
                                                                   const std::string& appId,
                                                                   const std::vector<std::string>& deviceEUIs)
    : NetworkServer(url, user, password, appId, deviceEUIs), client(url, "LoRaWANProxySource"), topicBase("v3/" + user),
      topicAll(topicBase + "/#"), topicDevice(topicBase + "/devices"), topicReceiveSuffix("/up"), topicSendSuffix("/down/push"),
      topicAllDevicesReceive(topicDevice + "/+" + topicReceiveSuffix) {}
LoRaWANProxySource::TheThingsNetworkServer::~TheThingsNetworkServer() = default;
std::string LoRaWANProxySource::TheThingsNetworkServer::toString() {
    std::stringstream ss;
    ss << "TheThingsNetworkServer => {\n";
    ss << "NetworkServer: " << NetworkServer::toString() << "\n";
    ss << "MQTT client: " << client.get_client_id() << "\n";
    ss << "MQTT url: " << client.get_server_uri() << "\n";
    return ss.str();
}
//might be refactored out
bool LoRaWANProxySource::TheThingsNetworkServer::isConnected() { return client.is_connected(); }
bool LoRaWANProxySource::TheThingsNetworkServer::connect() {
    if (!client.is_connected()) {
        //open();
        try {
            //TODO: actually get authentication to work
            //            auto sslopt = mqtt::ssl_options_builder()
            //                              .ca_path(capath)
            //                              .trust_store(certpath)
            //                              .private_key(keypath)
            //                              .verify(false)
            //                              .enable_server_cert_auth(false)
            //                              .finalize();
            //            //automatic reconnect = true enables establishing a connection with a broker again, after a disconnect
            auto connOpts = mqtt::connect_options_builder()
                                //.ssl(sslopt)
                                .user_name(user)
                                .password(password)
                                .automatic_reconnect(true)
                                .clean_session(true)
                                .finalize();

            // Start consumer before connecting to make sure to not miss messages
            client.start_consuming();

            // Connect to the server
            NES_DEBUG("LoRaWANProxySource::connect Connecting to the MQTT server...");
            auto rsp = client.connect(connOpts);

            // If there is no session present, then we need to subscribe, but if
            // there is a session, then the server remembers us and our
            // subscriptions.
            if (!rsp.is_session_present()) {
                auto resp = client.subscribe(topicAllDevicesReceive, 0);
                //TODO: handle errors
            }
        } catch (const mqtt::exception& error) {
            NES_ERROR("LoRaWANProxySource::connect:{} {} {} {}",
                      error.what(),
                      error.to_string(),
                      error.get_message(),
                      error.get_reason_code_str());
            return false;
        }
    }
    return true;
}
bool LoRaWANProxySource::TheThingsNetworkServer::disconnect() {
    NES_DEBUG("LoRaWANProxySource::disconnect connected=", client.is_connected());
    if (client.is_connected()) {
        //close();
        NES_DEBUG("LoRaWANProxySource: Shutting down and disconnecting from the MQTT server.");

        client.unsubscribe(topicAllDevicesReceive);
        //TODO: Handle Errors
        client.disconnect();
        NES_DEBUG("LoRaWANProxySource::disconnect: disconnected.");
    } else {
        NES_DEBUG("LoRaWANProxySource::disconnect: Client was already disconnected");
    }

    if (!client.is_connected()) {
        NES_DEBUG("LoRaWANProxySource::disconnect: TheThingsNetworkServer: disconnected");
    } else {
        NES_DEBUG("LoRaWANProxySource::disconnect: TheThingsNetworkServer: NOT disconnected");
        return false;
    }
    return true;
}
EndDeviceProtocol::Output LoRaWANProxySource::TheThingsNetworkServer::receiveData() {
    NES_DEBUG("LoRaWANProxySource TheThingsNetworkServer: receiveData");

    if (!isConnected()) {//connects if not connected, or return true if already connected. Might be bad design
        connect();
    }
    NES_TRACE("Connected and listening for mqtt messages for 1 sec")
    auto output = EndDeviceProtocol::Output();
    auto count = 0;
    while (true) {
        auto msgReceived = client.consume_message();
        if (!msgReceived) {
            NES_TRACE("no messages received")
            break;//Exit if no new messages
        }
        ++count;
        auto rcvStr = msgReceived->get_payload_str();
        std::string rcvTopic = msgReceived->get_topic();
        NES_TRACE("Received msg no. {} on topic: \"{}\" with payload: \"{}\"", count, rcvTopic, rcvStr);

        // v3/{application id}@{tenant id}/devices/{device id}/EVENT
        auto substrings = Util::splitWithStringDelimiter<std::string>(rcvTopic, "/");
        auto devEUI = substrings[3];
        auto event = substrings[5];

        //rcvStr should be formatted as shown here: https://www.thethingsindustries.com/docs/integrations/mqtt/#example under "show uplink message example"
        //most important is data which is placed on the "data" field
        try {
            auto js = nlohmann::json::parse(rcvStr);
            if (!js.contains("uplink_message")) {
                NES_WARNING("LoRaWANProxySource: parsed json does not contain uplink_message field");
            } else if (!js["uplink_message"].contains("frm_payload")) {
                NES_WARNING("LoRaWANProxySource: parsed json does not contain uplink_message.frm_payload field");
            } else {
                output.ParseFromString(base64_decode(js["uplink_message"]["frm_payload"]));
            }

        } catch (nlohmann::json::parse_error& ex) {
            NES_WARNING("LoRaWANProxySource: JSON parse error {}", ex.what());
        }
    }

    return output;
}

bool LoRaWANProxySource::TheThingsNetworkServer::sendMessage(EndDeviceProtocol::Message message) {
    auto pbEncoded = base64_encode(message.SerializeAsString());

    for (const auto& devEUI : deviceEUIs) {
        NES_DEBUG("{}",devEUI);
        //JSON payload must conform to:
        //        {
        //            "downlinks": [{
        //                "f_port": 15,
        //                "frm_payload": "vu8=",
        //                "priority": "HIGH",
        //                "confirmed": true,
        //                "correlation_ids": ["my-correlation-id"]
        //            }]
        //        }

        nlohmann::json payload{{"downlinks",
                                nlohmann::json::array({{
                                    {"f_port", 1},
                                    {"frm_payload", pbEncoded},
                                    //{"priority", "HIGH"},
                                    {"confirmed", true},
                                    //{"correlation_ids": ["my-correlation-id"]}
                                }})}};
        auto topic = topicDevice + "/" + devEUI + topicSendSuffix;
        auto payload_dump = payload.dump();
        NES_DEBUG("sending data: {} to topic: {} with payload {}", message.DebugString(), topic,payload.dump());
        
        client.publish(mqtt::make_message(topic, payload_dump));
    }
    return true;//TODO: Handle errors
}

#pragma endregion ttnstack

LoRaWANProxySource::LoRaWANProxySource(const SchemaPtr& schema,
                                       const Runtime::BufferManagerPtr& bufferManager,
                                       const Runtime::QueryManagerPtr& queryManager,
                                       const LoRaWANProxySourceTypePtr& sourceConfig,
                                       OperatorId operatorId,
                                       OriginId originId,
                                       size_t numSourceLocalBuffers,
                                       GatheringMode gatheringMode,
                                       const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& executableSuccessors)
    : DataSource(schema,
                 bufferManager,
                 queryManager,
                 operatorId,
                 originId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 "LoRaWANProxySource",
                 executableSuccessors),
      sourceConfig(sourceConfig),
      server(*new TheThingsNetworkServer(sourceConfig->getUrl()->getValue(),
                                         sourceConfig->getUserName()->getValue(),
                                         sourceConfig->getPassword()->getValue(),
                                         sourceConfig->getAppId()->getValue(),
                                         sourceConfig->getDeviceEUIs()->getValue())){
          //    topicBase = "application/" + sourceConfig->getAppId()->getValue();
          //    topicAll = topicBase + "/#";
          //    topicDevice = topicBase + "/device";
          //    topicReceiveSuffix = "/event/up";
          //    topicSendSuffix = "/command/down";
          //    topicAllDevicesReceive = topicDevice + "/+" + topicReceiveSuffix;
          //    capath = sourceConfig->getCapath()->getValue();
          //    certpath = sourceConfig->getCertpath()->getValue();
          //    keypath = sourceConfig->getKeypath()->getValue();

          //region initialize parser
          //    std::vector<std::string> schemaKeys;
          //    std::vector<PhysicalTypePtr> physicalTypes;
          //    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory;
          //
          //    for (const auto& field : schema->fields) {
          //        auto physField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
          //        physicalTypes.push_back(physField);
          //        auto fieldName = field->getName();
          //        if (fieldName.find('$') == std::string::npos) {
          //            schemaKeys.push_back(fieldName);
          //        } else {// if fieldname contains a $ the name is logicalstream$fieldname
          //            schemaKeys.push_back(fieldName.substr(fieldName.find('$') + 1, fieldName.size() - 1));
          //        }
          //    }
          //jsonParser = std::make_unique<JSONParser>(schema->getSize(), schemaKeys, physicalTypes);
          //endregion
          NES_DEBUG("LoRaWANProxySource::LoRaWANProxySource()")} LoRaWANProxySource::~LoRaWANProxySource() {
    NES_DEBUG("LoRaWANProxySource::~LoRaWANProxySource()");
    bool success = disconnect();
    if (success) {
        NES_DEBUG("LoRaWANProxySource::~LoRaWANProxySource: Destroy LoRaWANProxySource");
    } else {
        NES_ERROR(
            "LoRaWANProxySource::~LoRaWANProxySource: Destroy LoRaWANProxySource failed cause it could not be disconnected");
        assert(0);
    }
    NES_DEBUG("LoRaWANProxySource::~LoRaWANProxySource : Destroy LoRaWANProxySource");
}

bool LoRaWANProxySource::connect() {
    if (!server.isConnected()) {
        server.connect();
        open();
        //        try {
        //            //TODO: actually get authentication to work
        //            //            auto sslopt = mqtt::ssl_options_builder()
        //            //                              .ca_path(capath)
        //            //                              .trust_store(certpath)
        //            //                              .private_key(keypath)
        //            //                              .verify(false)
        //            //                              .enable_server_cert_auth(false)
        //            //                              .finalize();
        //            //            //automatic reconnect = true enables establishing a connection with a broker again, after a disconnect
        //            auto connOpts = mqtt::connect_options_builder()
        //                                //.ssl(sslopt)
        //                                .automatic_reconnect(true)
        //                                .clean_session(true)
        //                                .finalize();
        //
        //            // Start consumer before connecting to make sure to not miss messages
        //            client->start_consuming();
        //
        //            // Connect to the server
        //            NES_DEBUG("LoRaWANProxySource::connect Connecting to the MQTT server...");
        //            auto tok = client->connect(connOpts);
        //
        //            // Getting the connect response will block waiting for the
        //            // connection to complete.
        //            auto rsp = tok->get_connect_response();
        //
        //            // If there is no session present, then we need to subscribe, but if
        //            // there is a session, then the server remembers us and our
        //            // subscriptions.
        //            if (!rsp.is_session_present()) {
        //                client->subscribe(topicAllDevicesReceive, 0)->wait();
        //            }
        //        } catch (const mqtt::exception& error) {
        //            NES_ERROR("LoRaWANProxySource::connect: " << error);
        //            return false;
        //        }

        if (server.isConnected()) {
            NES_DEBUG("LoRaWANProxySource::connect: Connection established with topic: ");
            NES_DEBUG("LoRaWANProxySource::connect: connected");

            NES_DEBUG("Sending Queries...");
            sendQueries();
        } else {
            NES_DEBUG("LoRaWANProxySource::connect: NOT connected");
        }
    }
    return true;
}

bool LoRaWANProxySource::disconnect() {
    NES_DEBUG("LoRaWANProxySource::disconnect connected={}", server.isConnected());
    if (server.isConnected()) {
        server.disconnect();
    } else {
        NES_DEBUG("LoRaWANProxySource::disconnect: Client was already disconnected");
    }

    if (!server.isConnected()) {
        NES_DEBUG("LoRaWANProxySource::disconnect: disconnected");
    } else {
        NES_DEBUG("LoRaWANProxySource::disconnect: NOT disconnected");
        return false;
    }
    return true;
}

std::optional<Runtime::TupleBuffer> LoRaWANProxySource::receiveData() {
    NES_DEBUG("LoRaWANProxySource: receiveData");
    auto buffer = allocateBuffer();
    int count = 0;
    connect();//connects if not connected, or return true if already connected. Might be bad design
    NES_TRACE("Connected and listening for mqtt messages for 1 sec")
    auto output = server.receiveData();
    for (const auto& queryResponse : output.responses()) {
        auto id = queryResponse.id();
        auto query = sourceConfig->getSerializedQueries()->at(runningQueries[id]);
        //TODO: actually use the resulttype
        auto resultArray = queryResponse.response();
        auto tupCount = buffer.getNumberOfTuples();
        for (int i = 0; i < resultArray.size(); ++i) {
            auto result = resultArray[i];
            auto bufferCell = buffer[tupCount][i];
            if (result.has__int8())
                bufferCell.write<int8_t>(result._int8());
            if (result.has__int16())
                bufferCell.write<int16_t>(result._int16());
            if (result.has__int32())
                bufferCell.write<int32_t>(result._int32());
            if (result.has__int64())
                bufferCell.write<int64_t>(result._int64());

            if (result.has__uint8())
                bufferCell.write<u_int8_t>(result._uint8());
            if (result.has__uint16())
                bufferCell.write<u_int16_t>(result._uint16());
            if (result.has__uint32())
                bufferCell.write<u_int32_t>(result._uint32());
            if (result.has__uint64())
                bufferCell.write<u_int64_t>(result._uint64());

            if (result.has__float())
                bufferCell.write<float_t>(result._float());
            if (result.has__double())
                bufferCell.write<float_t>(result._double());
        }
        buffer.setNumberOfTuples(buffer.getNumberOfTuples() + 1);
    }
    ++count;

    buffer.setNumberOfTuples(count);
    return buffer.getBuffer();
}

std::string LoRaWANProxySource::toString() const {
    std::stringstream ss;
    ss << "LoRaWANProxySource(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    ss << "CONFIG(" << sourceConfig->toString() << ")";
    ss << ").";
    return ss.str();
}
SourceType LoRaWANProxySource::getType() const { return SourceType::LORAWAN_SOURCE; }
bool LoRaWANProxySource::sendQueries() {
    auto queryMap = sourceConfig->getSerializedQueries();
    auto pbMsg = EndDeviceProtocol::Message();
    std::vector<EndDeviceProtocol::Query> queries;

    for (const auto& [qid, query] : *queryMap) {
        auto serializedQuery = EndDeviceProtocol::Query();
        serializedQuery.CopyFrom(*query);
        queries.push_back(serializedQuery);
        runningQueries.push_back(qid);
    }
    pbMsg.mutable_queries()->Assign(queries.begin(), queries.end());
    auto res = server.sendMessage(pbMsg);
    return res;
}
const LoRaWANProxySourceTypePtr& LoRaWANProxySource::getSourceConfig() const { return sourceConfig; }

//debug action listener
//LoRaWANProxySource::debug_action_listener::debug_action_listener(std::string name) : name(std::move(name)) {}
//void LoRaWANProxySource::debug_action_listener::on_failure(const mqtt::token& asyncActionToken) {
//    NES_DEBUG(name << ": failure for mqtt token " << asyncActionToken.get_message_id())
//}
//void LoRaWANProxySource::debug_action_listener::on_success(const mqtt::token& asyncActionToken) {
//    NES_DEBUG(name << " : success for mqtt token " << asyncActionToken.get_message_id() << " with topics "
//                   << asyncActionToken.get_topics());
//}
//void LoRaWANProxySource::callback::connected(const mqtt::string& string) { NES_DEBUG("mqtt connected callback"); }
//void LoRaWANProxySource::callback::connection_lost(const mqtt::string& string) { NES_DEBUG("mqtt connection failed callback"); }
//void LoRaWANProxySource::callback::message_arrived(mqtt::const_message_ptr ptr) {
//    ptr->get_topic(); }
//void LoRaWANProxySource::callback::delivery_complete(mqtt::delivery_token_ptr ptr) { NES_DEBUG("delivery completed callback"); }
}// namespace NES

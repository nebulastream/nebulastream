//
// Created by Kasper Hjort Berthelsen on 10/11/2022.
//

#include <API/AttributeField.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/LoRaWANProxySource.hpp>
#include <Sources/Parsers/JSONParser.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <nlohmann/json.hpp>
#include <utility>

namespace NES {
//class NetworkServer {
//  public:
//    NetworkServer(const std::string& url, const std::string& user, const std::string& appId, const std::vector<std::string>& deviceEuIs)
//        : url(url), user(user), appId(appId), deviceEUIs(deviceEuIs) {}
//
//    // Functions to implement
//    virtual ~NetworkServer() = default;;
//    virtual bool connect() = 0;
//    virtual bool isConnected() = 0;
//    virtual bool disconnect() = 0;
//    virtual EndDeviceProtocol::Output receiveData() = 0;
//    virtual bool sendMessage(EndDeviceProtocol::Message) = 0;
//
//  protected:
//    std::string url;
//    std::string user;
//    std::string appId;
//    std::vector<std::string> deviceEUIs;
//};

LoRaWANProxySource::NetworkServer::NetworkServer(const std::string& url,
                                                 const std::string& user,
                                                 const std::string& appId,
                                                 std::vector<std::string>& deviceEUIs)
    : url(url), user(user), appId(appId), deviceEUIs(deviceEUIs) {}

LoRaWANProxySource::ChirpStackServer::ChirpStackServer(const std::string& url,
                                                       const std::string& user,
                                                       const std::string& appId,
                                                       std::vector<std::string>& deviceEUIs)
    : NetworkServer(url, user, appId, deviceEUIs), client(url, "LoRaWANProxySource"), topicBase("application/" + appId),
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
            NES_ERROR("LoRaWANProxySource::connect: " << error);
            return false;
        }
    }
    return true;
}
bool LoRaWANProxySource::ChirpStackServer::disconnect() {
    NES_DEBUG("LoRaWANProxySource::disconnect connected=" << client.is_connected());
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
        NES_DEBUG("LoRaWANProxySource::disconnect:  " << this << ": disconnected");
    } else {
        NES_DEBUG("LoRaWANProxySource::disconnect:  " << this << ": NOT disconnected");
        return false;
    }
    return true;
}
EndDeviceProtocol::Output LoRaWANProxySource::ChirpStackServer::receiveData() {
    NES_DEBUG("LoRaWANProxySource " << this << ": receiveData");

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
        NES_TRACE("Received msg no. " << count << " on topic: \"" << rcvTopic << "\" with payload: \"" << rcvStr << "\"")
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
                output.ParseFromString(Util::base64Decode(js["data"]));
            }
        } catch (nlohmann::json::parse_error& ex) {
            NES_WARNING("LoRaWANProxySource: JSON parse error " << ex.what());
        }
    }
    return output;
}

bool LoRaWANProxySource::ChirpStackServer::sendMessage(EndDeviceProtocol::Message message) {
    auto pbEncoded = Util::base64Encode(message.SerializeAsString());

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
        NES_DEBUG("sending data: " + message.DebugString() + " to topic: " + topic + " with payload " + payload.dump());
        client.publish(mqtt::make_message(topic, payload_dump));
    }
    return true;//TODO: Handle errors
}
LoRaWANProxySource::ChirpStackServer::~ChirpStackServer() {}
LoRaWANProxySource::ChirpStackServer::ChirpStackEvent LoRaWANProxySource::ChirpStackServer::strToEvent(const std::string& s) { return magic_enum::enum_cast<ChirpStackEvent>(s).value(); }
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
                 executableSuccessors),
      sourceConfig(sourceConfig), url(sourceConfig->getUrl()->getValue()), user(sourceConfig->getUserName()->getValue()),
      appId(sourceConfig->getAppId()->getValue()), deviceEUIs(sourceConfig->getDeviceEUIs()->getValue()), server(*new ChirpStackServer(url, user, appId, deviceEUIs)) {
    topicBase = "application/" + sourceConfig->getAppId()->getValue();
    topicAll = topicBase + "/#";
    topicDevice = topicBase + "/device";
    topicReceiveSuffix = "/event/up";
    topicSendSuffix = "/command/down";
    topicAllDevicesReceive = topicDevice + "/+" + topicReceiveSuffix;
    capath = sourceConfig->getCapath()->getValue();
    certpath = sourceConfig->getCertpath()->getValue();
    keypath = sourceConfig->getKeypath()->getValue();

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
    NES_DEBUG("LoRaWANProxySource::LoRaWANProxySource()")
}
LoRaWANProxySource::~LoRaWANProxySource() {
    NES_DEBUG("LoRaWANProxySource::~LoRaWANProxySource()");
    bool success = disconnect();
    if (success) {
        NES_DEBUG("LoRaWANProxySource::~LoRaWANProxySource  " << this << ": Destroy LoRaWANProxySource");
    } else {
        NES_ERROR("LoRaWANProxySource::~LoRaWANProxySource  "
                  << this << ": Destroy LoRaWANProxySource failed cause it could not be disconnected");
        assert(0);
    }
    NES_DEBUG("LoRaWANProxySource::~LoRaWANProxySource  " << this << ": Destroy LoRaWANProxySource");
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
            NES_DEBUG("LoRaWANProxySource::connect: Connection established with topic: " << topicAll);
            NES_DEBUG("LoRaWANProxySource::connect:  " << this << ": connected");

            NES_DEBUG("Sending Queries...");
            sendQueries();
        } else {
            NES_DEBUG("LoRaWANProxySource::connect:  " << this << ": NOT connected");
        }
    }
    return true;
}

bool LoRaWANProxySource::disconnect() {
    NES_DEBUG("LoRaWANProxySource::disconnect connected=" << server.isConnected());
    if (server.isConnected()) {
        server.disconnect();
    } else {
        NES_DEBUG("LoRaWANProxySource::disconnect: Client was already disconnected");
    }

    if (!server.isConnected()) {
        NES_DEBUG("LoRaWANProxySource::disconnect:  " << this << ": disconnected");
    } else {
        NES_DEBUG("LoRaWANProxySource::disconnect:  " << this << ": NOT disconnected");
        return false;
    }
    return true;
}

std::optional<Runtime::TupleBuffer> LoRaWANProxySource::receiveData() {
    NES_DEBUG("LoRaWANProxySource " << this << ": receiveData");
    auto buffer = allocateBuffer();
    int count = 0;
    if (connect()) {//connects if not connected, or return true if already connected. Might be bad design
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
        }
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

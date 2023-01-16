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

enum class ChirpStackEvent { UP, STATUS, JOIN, ACK, TXACK, LOG, LOCATION, INTEGRATION };

ChirpStackEvent strToEvent(const std::string& s) { return magic_enum::enum_cast<ChirpStackEvent>(s).value(); };

LoRaWANProxySource::LoRaWANProxySource(const SchemaPtr& schema,
                                       const Runtime::BufferManagerPtr& bufferManager,
                                       const Runtime::QueryManagerPtr& queryManager,
                                       const LoRaWANProxySourceTypePtr& sourceConfig,
                                       OperatorId operatorId,
                                       OriginId originId,
                                       size_t numSourceLocalBuffers,
                                       GatheringMode::Value gatheringMode,
                                       const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& executableSuccessors)
    : DataSource(schema,
                 bufferManager,
                 queryManager,
                 operatorId,
                 originId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 executableSuccessors),
      sourceConfig(sourceConfig) {
    client = std::make_shared<mqtt::async_client>(sourceConfig->getUrl()->getValue(), "LoRaWANProxySource");
    user = sourceConfig->getUserName()->getValue();
    topic = "application/" + sourceConfig->getAppId()->getValue() + "/#";

    //region initialize parser
    std::vector<std::string> schemaKeys;
    std::vector<PhysicalTypePtr> physicalTypes;
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory;

    for (const auto& field : schema->fields) {
        auto physField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physField);
        auto fieldName = field->getName();
        if (fieldName.find('$') == std::string::npos) {
            schemaKeys.push_back(fieldName);
        } else {// if fieldname contains a $ the name is logicalstream$fieldname
            schemaKeys.push_back(fieldName.substr(fieldName.find('$') + 1, fieldName.size() - 1));
        }
    }
    jsonParser = std::make_unique<JSONParser>(schema->getSize(), schemaKeys, physicalTypes);
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
    if (!client->is_connected()) {
        open();
        try {
            //automatic reconnect = true enables establishing a connection with a broker again, after a disconnect
            auto connOpts =
                mqtt::connect_options_builder().user_name(user).automatic_reconnect(true).clean_session(true).finalize();

            // Start consumer before connecting to make sure to not miss messages
            client->start_consuming();

            // Connect to the server
            NES_DEBUG("LoRaWANProxySource::connect Connecting to the MQTT server...");
            auto tok = client->connect(connOpts);

            // Getting the connect response will block waiting for the
            // connection to complete.
            auto rsp = tok->get_connect_response();

            // If there is no session present, then we need to subscribe, but if
            // there is a session, then the server remembers us and our
            // subscriptions.
            if (!rsp.is_session_present()) {
                client->subscribe(topic, 0)->wait();
            }
        } catch (const mqtt::exception& error) {
            NES_WARNING("LoRaWANProxySource::connect: " << error);
            return false;
        }

        if (client->is_connected()) {
            NES_DEBUG("LoRaWANProxySource::connect: Connection established with topic: " << topic);
            NES_DEBUG("LoRaWANProxySource::connect:  " << this << ": connected");
        } else {
            NES_DEBUG("LoRaWANProxySource::connect:  " << this << ": NOT connected");
        }
    }
    return true;
}

bool LoRaWANProxySource::disconnect() {
    NES_DEBUG("LoRaWANProxySource::disconnect connected=" << client->is_connected());
    if (client->is_connected()) {
        //close();
        NES_DEBUG("LoRaWANProxySource: Shutting down and disconnecting from the MQTT server.");

        client->unsubscribe(topic)->wait();

        client->disconnect()->wait();
        NES_DEBUG("LoRaWANProxySource::disconnect: disconnected.");
    } else {
        NES_DEBUG("LoRaWANProxySource::disconnect: Client was already disconnected");
    }

    if (!client->is_connected()) {
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
    if (connect()) {//connects if not connected, or return true if already connected. Might be bad design
        NES_TRACE("Connected and listening for mqtt messages for 1 sec")

        unsigned int count = 0;
        while (true) {
            mqtt::const_message_ptr msg;
            auto msgRcvd = client->try_consume_message_for(&msg, std::chrono::seconds(1));
            if (!msgRcvd) {
                NES_TRACE("no messages received")
                break;//Exit if no new messages
            }

            auto rcvStr = msg->get_payload_str();
            auto rcvTopic = msg->get_topic();
            NES_TRACE("Received msg no. " << count << " on topic: \"" << rcvTopic << "\" with payload: \"" << rcvStr << "\"")
            // 0                                35            58
            // |                                |             |
            // application/APPLICATION_ID/device/DEV_EUI/event/EVENT
            auto devEUI = rcvTopic.substr(36, 64 / 4);
            auto event = rcvTopic.substr(59, rcvTopic.length());

            //rcvStr should be formatted as shown here: https://www.chirpstack.io/docs/chirpstack/integrations/events.html#up---uplink-event
            //most important is data which is placed on the "data" field
            try {
                auto js = nlohmann::json::parse(rcvStr);

                if (!js.contains("data")) {
                    NES_WARNING("LoRaWANProxySource: parsed json does not contain data field");
                } else {

                    jsonParser->writeInputTupleToTupleBuffer(js["data"], 0, buffer, schema, localBufferManager);
                    //TODO: this is a blocking call, which should get and return data to
                    //      the caller. But where should we handle control msg or other msgs
                    //      should prob. impl. async handling of msgs
                    ++count;
                }
            } catch (nlohmann::json::parse_error& ex) {
                NES_WARNING("LoRaWANProxySource: JSON parse error " << ex.what());
            }
        }
        buffer.setNumberOfTuples(count);
    }
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
SourceType LoRaWANProxySource::getType() const { return LORAWAN_SOURCE; }

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

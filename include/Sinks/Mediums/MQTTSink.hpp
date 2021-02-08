//
// Created by rudi on 01.02.21.
//

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

#ifndef MQTTSINK_HPP
#define MQTTSINK_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <zmq.hpp>

#include <Sinks/Mediums/SinkMedium.hpp>
#include <mqtt/client.h>
//#include "mqtt/client.h"
#include "mqtt/async_client.h"


namespace NES {


class UserCallback : public virtual mqtt::callback
{
    void connection_lost(const std::string& cause) override;

    void delivery_complete(mqtt::delivery_token_ptr tok) override;

  public:
};

class MQTTSink : public SinkMedium {

  public:
    //TODO: remove internal flag once the new network stack is in place
    MQTTSink(SinkFormatPtr sinkFormat, QuerySubPlanId parentPlanId, const std::string& host,
             const uint16_t port, const std::string& clientId, const std::string& topic, const std::string& user,
             const uint32_t maxBufferedMSGs = 60, char timeUnit = 'm', uint64_t msgDelay = 500, bool asynchronousClient = 1);
    ~MQTTSink();

    bool writeData(NodeEngine::TupleBuffer& input_buffer, NodeEngine::WorkerContextRef) override;
    void setup() override { connect(); };
    void shutdown() override{};

    bool connect();
    bool disconnect();

    /**
     * @brief Get MQTT sink port
     */
    int getPort();

    /**
     * @brief Get MQTT host name
     */
    const std::string& getHost() const;

    /**
     * @brief Get MQTT topic
     */
    const std::string& getTopic() const;

    /**
     * @brief Get MQTT clientId
     */
    const std::string& getClientId() const;

    /**
     * @brief Get MQTT user
     */
    const std::string& getUser() const;


    /**
     * @brief Get connected status of MQTT sink
     */
    bool getConnected();

    /**
     * @brief Print MQTT Sink (schema, host, port, clientId, topic, user)
     */
    const std::string toString() const override;

    /**
    * @brief method to return the type of medium
    * @return type of medium
    */
    SinkMediumTypes getSinkMediumType();

  private:
    MQTTSink();

    QuerySubPlanId parentPlanId;

    std::string host;
    uint16_t port;

    std::string clientId;
    std::string topic;

    std::string user;

    uint32_t maxBufferedMSGs;
    uint8_t timeUnit; //'n'-nanoseconds, 'm'-milliseconds, 's'-seconds

    uint64_t msgDelay;
    bool asynchronousClient;
    char qualityOfService; // 0-at most once, 1-at least once, 2-exactly once

    std::string address;

    bool connected;
    std::chrono::duration<int64_t, std::ratio<1, 1000000000>> sendDuration;

    struct ClientWrapper {
        mqtt::async_client_ptr asyncClient;
        mqtt::client_ptr syncClient;

        bool useAsyncClient;

      public:
        ClientWrapper(bool useAsyncClient, const std::string& address, const std::string& clientId, uint32_t maxBufferedMSGs){
            this->useAsyncClient = useAsyncClient;
            if(useAsyncClient) {
                asyncClient = std::make_shared<mqtt::async_client>(address, clientId, maxBufferedMSGs);
            } else {
                syncClient = std::make_shared<mqtt::client>(address, clientId, maxBufferedMSGs);
            }
        }
        mqtt::async_client_ptr getAsyncClient() {
            return (useAsyncClient) ? asyncClient : nullptr;
        }
        mqtt::client_ptr getSyncClient() {
            return (useAsyncClient) ? nullptr : syncClient;
        }

        void connect(mqtt::connect_options connOpts) {
            if(useAsyncClient) { asyncClient->connect(connOpts)->wait(); } else { syncClient->connect(); }
        }
        void disconnect() {
            if(useAsyncClient) { asyncClient->disconnect()->wait(); } else { syncClient->disconnect(); }
        }
        int getBufferSize() {
            return (asyncClient) ? asyncClient->get_pending_delivery_tokens().size() : 0;
        }

        void setCallback(UserCallback& cb) {
            syncClient->set_callback(cb);
        }
    };
    ClientWrapper client;
//    mqtt::async_client client1;
//    mqtt::client client2;
//    mqtt::client_ptr clientPtr;
// not needed yet, but possibly in the future:

//    std::string& persistDir;
//    std::string& period;
//    std::string& password;

};
//TODO change to MQTT specific
typedef std::shared_ptr<MQTTSink> MQTTSinkPtr;


}// namespace NES


#endif// MQTTSINK_HPP

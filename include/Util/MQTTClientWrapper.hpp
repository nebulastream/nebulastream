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

#ifndef NES_INCLUDE_UTIL_MQTT_CLIENT_WRAPPER_HPP_
#define NES_INCLUDE_UTIL_MQTT_CLIENT_WRAPPER_HPP_

#include <Util/Logger.hpp>
#include <mqtt/callback.h>
#include <mqtt/client.h>

namespace NES {
/**
 * @brief Helper class for MQTTSink. Provides a convenient way to choose between asynchronous and synchronous clients
 */
class MQTTClientWrapper {
  public:
    /**
     * @brief Creates the MQTTClientWrapper
     * @param useAsyncClient: if true: use asynchronous client, else: use synchronous client
     * @param address: address name of MQTT broker
     * @param clientId: client ID for MQTT client
     * @param maxBufferedMSGs: maximal number of messages that can be buffered by the client before disconnecting
     * @return MQTTClientWrapper
     */
    MQTTClientWrapper(bool useAsyncClient,
                      std::string const& address,
                      std::string const& clientId,
                      uint64_t maxBufferedMSGs,
                      std::string const& topic,
                      int qualityOfService);

    /**
     * @brief get a pointer to the MQTT asynchronous client ONLY IF 'useAsyncClient' = true
     */
    mqtt::async_client_ptr getAsyncClient();

    /**
     * @brief get a pointer to the MQTT synchronous client ONLY IF 'useAsyncClient' = false
     */
    mqtt::client_ptr getSyncClient();

    /**
     * @brief connect to an MQTT broker
     */
    void connect(mqtt::connect_options connOpts);

    /**
     * @brief disconnect from an MQTT broker
     */
    void disconnect();

    /**
    * @brief send a string to an MQTT broker
    */
    void sendPayload(std::string payload);

    /**
     * @brief get the number of elements currently residing in the buffer (messages that have not been delivered yet)
     */
    uint64_t getNumberOfUnsentMessages();

    /**
     * @brief Helper class for synchronous client - Defines how to interact with broker replies and connection loss
     */
    class UserCallback : public virtual mqtt::callback {
      public:
        void connection_lost(const std::string& cause) override;
        void delivery_complete(mqtt::delivery_token_ptr tok) override;
    };

    /**
     * @brief define a callback behaviour for the synchronous client
     */
    void setCallback(UserCallback& cb);

  private:
    mqtt::async_client_ptr asyncClient;
    mqtt::client_ptr syncClient;
    bool useAsyncClient;
    std::string topic;
    int qualityOfService;
    std::shared_ptr<mqtt::topic> sendTopic;
};
using MQTTClientWrapperPtr = std::shared_ptr<MQTTClientWrapper>;

}// namespace NES
#endif  // NES_INCLUDE_UTIL_MQTT_CLIENT_WRAPPER_HPP_

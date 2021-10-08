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
#include <Util/Logger.hpp>
#include <Util/MQTTClientWrapper.hpp>
#include <utility>

namespace NES {
const std::chrono::duration<int64_t> MAX_WAIT_FOR_BROKER_CONNECT = std::chrono::seconds(20);
MQTTClientWrapper::MQTTClientWrapper(bool useAsyncClient,
                                     const std::string& address,
                                     const std::string& clientId,
                                     uint64_t maxBufferedMSGs,
                                     const std::string& topic,
                                     int qualityOfService) {
    this->useAsyncClient = useAsyncClient;
    this->topic = topic;
    this->qualityOfService = qualityOfService;
    if (useAsyncClient) {
        asyncClient = std::make_shared<mqtt::async_client>(address, clientId, maxBufferedMSGs);
        sendTopic = std::make_shared<mqtt::topic>(*asyncClient, topic, qualityOfService, true);
    } else {
        syncClient = std::make_shared<mqtt::client>(address, clientId, maxBufferedMSGs);
    }
}

mqtt::async_client_ptr MQTTClientWrapper::getAsyncClient() { return useAsyncClient ? asyncClient : nullptr; }

mqtt::client_ptr MQTTClientWrapper::getSyncClient() { return useAsyncClient ? nullptr : syncClient; }

void MQTTClientWrapper::connect(mqtt::connect_options connOpts) {
    if (useAsyncClient) {
        asyncClient->connect(std::move(connOpts))->wait_for(MAX_WAIT_FOR_BROKER_CONNECT);
    } else {
        syncClient->connect();
    }
}

void MQTTClientWrapper::disconnect() {
    if (useAsyncClient) {
        asyncClient->disconnect()->wait_for(MAX_WAIT_FOR_BROKER_CONNECT);
    } else {
        syncClient->disconnect();
    }
}

void MQTTClientWrapper::sendPayload(std::string payload) {
    if (asyncClient) {
        //qualityOfService to enable cleanSessions(require > 0), retained not necessary (broker can store up to 1
        // -> retained message, which is delivered to a newly subscribed client(correct topic) first)
        sendTopic->publish(std::move(payload), qualityOfService, false);
    } else {
        auto pubmsg = mqtt::make_message(topic, payload);
        pubmsg->set_qos(qualityOfService);
        (*syncClient).publish(pubmsg);
    }
}

uint64_t MQTTClientWrapper::getNumberOfUnsentMessages() {
    return asyncClient ? asyncClient->get_pending_delivery_tokens().size() : 0;
}

void MQTTClientWrapper::setCallback(UserCallback& cb) { syncClient->set_callback(cb); }

void MQTTClientWrapper::UserCallback::connection_lost(const std::string& cause) {
    NES_TRACE("MQTTClientWrapper::UserCallback::connection_lost: Connection lost");
    if (!cause.empty()) {
        NES_DEBUG("MQTTClientWrapper::UserCallback:connection_lost: cause: " << cause);
    }
}
void MQTTClientWrapper::UserCallback::delivery_complete(mqtt::delivery_token_ptr tok) {
    NES_TRACE("\n\t[Delivery complete for token: " << (tok ? tok->get_message_id() : -1) << "]");
}
}// namespace NES
#endif//ENABLE_MQTT_BUILD

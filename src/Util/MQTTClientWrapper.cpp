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

#include <Util/MQTTClientWrapper.hpp>
#include <mqtt/client.h>
#include <Util/Logger.hpp>

#ifdef ENABLE_MQTT_BUILD
namespace NES {
MQTTClientWrapper::MQTTClientWrapper(bool useAsyncClient, const std::string& address, const std::string& clientId, uint32_t maxBufferedMSGs) {
    this->useAsyncClient = useAsyncClient;
    if (useAsyncClient) {
        asyncClient = std::make_shared<mqtt::async_client>(address, clientId, maxBufferedMSGs);
    } else {
        syncClient = std::make_shared<mqtt::client>(address, clientId, maxBufferedMSGs);
    }
}
mqtt::async_client_ptr MQTTClientWrapper::getAsyncClient() { return (useAsyncClient) ? asyncClient : nullptr; }
mqtt::client_ptr MQTTClientWrapper::getSyncClient() { return (useAsyncClient) ? nullptr : syncClient; }

void MQTTClientWrapper::connect(mqtt::connect_options connOpts) {
    if (useAsyncClient) {
        asyncClient->connect(connOpts)->wait();
    } else {
        syncClient->connect();
    }
}
void MQTTClientWrapper::disconnect() {
    if (useAsyncClient) {
        asyncClient->disconnect()->wait();
    } else {
        syncClient->disconnect();
    }
}
int MQTTClientWrapper::getNumberOfUnsentMessages() { return (asyncClient) ? asyncClient->get_pending_delivery_tokens().size() : 0; }

void MQTTClientWrapper::setCallback(UserCallback& cb) { syncClient->set_callback(cb); }
}

#endif

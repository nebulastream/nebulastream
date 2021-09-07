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

#include <chrono>
#include <thread>
#include <Mobility/LocationHTTPClient.h>
#include <Util/Logger.hpp>

namespace NES {

const int REQUEST_SUCCESS_CODE = 200;

LocationHTTPClientPtr LocationHTTPClient::create(const std::string& host, int port, const std::string& workerName, uint32_t updateInterval) {
    return  std::make_shared<LocationHTTPClient>(host, port, workerName, updateInterval);
}

NES::LocationHTTPClient::LocationHTTPClient(const std::string& host, int port, std::string  workerName, uint32_t updateInterval)
    : workerName(std::move(workerName)), sourcesEnabled(true),
      httpClient("http://" + host + ":" + std::to_string(port) + "/v1/nes"), running(false), updateInterval(updateInterval) {}

bool LocationHTTPClient::registerSource(uint32_t sourceRange) {
    std::lock_guard lock(clientLock);
    NES_DEBUG("LocationHTTPClient: register source");

    web::json::value msg{};
    msg["nodeId"] = web::json::value::string(workerName);
    msg["range"] = web::json::value::number(sourceRange);

    int statusCode = 0;

    httpClient.request(web::http::methods::POST, "/geo/source", msg)
        .then([&statusCode](const web::http::http_response& response) {
            statusCode = response.status_code();
            return response;
        })
        .wait();

    return (statusCode == REQUEST_SUCCESS_CODE);
}

bool LocationHTTPClient::fetchSourceStatus() {
    std::lock_guard lock(clientLock);
    NES_DEBUG("LocationHTTPClient: fetching info for source -> " << workerName);

    web::uri_builder builder("/geo/source");
    builder.append_query(("nodeId"), workerName);

    int statusCode = 0;
    web::json::value getSourceJsonReturn;

    httpClient.request(web::http::methods::GET, builder.to_string())
        .then([&statusCode](const web::http::http_response& response) {
            statusCode = response.status_code();
            return response.extract_json();
        })
        .then([&getSourceJsonReturn](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("get source: set return");
                getSourceJsonReturn = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("get source: error while setting return" << e.what());
            }
        })
        .wait();

    if (statusCode == REQUEST_SUCCESS_CODE) {
        sourcesEnabled = getSourceJsonReturn.at("enabled").as_bool();
    }

    NES_DEBUG("LocationHTTPClient: sourceEnabled -> " << sourcesEnabled);

    return (statusCode == REQUEST_SUCCESS_CODE);
}

bool LocationHTTPClient::areSourcesEnabled() {
    std::lock_guard lock(clientLock);
    return sourcesEnabled;
}

void LocationHTTPClient::setSourcesEnabled(bool sourcesEnabledValue) {
    std::lock_guard lock(clientLock);
    LocationHTTPClient::sourcesEnabled = sourcesEnabledValue;
}

void LocationHTTPClient::start() {
    this->running = true;

    while (running) {
        NES_DEBUG("LocationClient: fetching source status ...");
        bool success = fetchSourceStatus();
        NES_DEBUG("LocationClient: fetching source status ... " << success);
        std::this_thread::sleep_for(std::chrono::milliseconds(updateInterval));
    }

    NES_DEBUG("LocationClient: stopped!");
}

void LocationHTTPClient::stop() {
    NES_DEBUG("LocationClient: stopping ...");
    this->running = false;
}

}// namespace NES
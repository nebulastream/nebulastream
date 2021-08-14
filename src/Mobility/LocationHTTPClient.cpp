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

#include <Mobility/LocationHTTPClient.h>
#include <Util/Logger.hpp>

namespace NES {

const int REQUEST_SUCCESS_CODE = 200;

NES::LocationHTTPClient::LocationHTTPClient(const std::string& host, int port) : httpClient(
    "http://" + host + ":" + std::to_string(port) + "/v1/nes") {}

bool LocationHTTPClient::addSink(std::string nodeId, double movingRange) {
    NES_DEBUG("LocationHTTPClient: add sink");

    web::json::value msg{};
    msg["movingRange"] =  web::json::value::number(movingRange);
    msg["nodeId"] =  web::json::value::string(std::move(nodeId));

    int statusCode = 0;

    httpClient.request(web::http::methods::POST, "/geo/sink", msg)
    .then([&statusCode](const web::http::http_response& response) {
        statusCode = response.status_code();
        return response;
    })
    .wait();

    return (statusCode == REQUEST_SUCCESS_CODE);
}

bool LocationHTTPClient::addSource(std::string nodeId) {
    NES_DEBUG("LocationHTTPClient: add source");

    web::json::value msg{};
    msg["nodeId"] =  web::json::value::string(std::move(nodeId));

    int statusCode = 0;

    httpClient.request(web::http::methods::POST, "/geo/source", msg)
    .then([&statusCode](const web::http::http_response& response) {
        statusCode = response.status_code();
        return response;
    })
    .wait();

    return (statusCode == REQUEST_SUCCESS_CODE);
}

}
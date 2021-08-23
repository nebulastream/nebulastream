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

#ifndef NES_LOCATIONHTTPCLIENT_H
#define NES_LOCATIONHTTPCLIENT_H

#include <mutex>
#include <string>
#include <cpprest/http_client.h>

namespace NES {

class LocationHTTPClient;
using LocationHTTPClientPtr = std::shared_ptr<LocationHTTPClient>;

class LocationHTTPClient {
  private:
    std::string workerName;
    bool sourcesEnabled;
    web::http::client::http_client httpClient;
    bool running;

    std::mutex clientLock;

  public:
    static LocationHTTPClientPtr create(const std::string& host, int port, const std::string& workerName);
    explicit LocationHTTPClient(const std::string& host, int port, std::string  workerName);
    bool registerSource();
    bool fetchSourceStatus();

    void start();
    void stop();

    [[nodiscard]] bool areSourcesEnabled();
    void setSourcesEnabled(bool sourcesEnabled);
};


}

#endif//NES_LOCATIONHTTPCLIENT_H

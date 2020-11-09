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

#include "REST/NetworkUtils.hpp"

namespace NES {

HostInetInfo NetworkUtils::queryHostInetInfo() {
    io_service ios;
    tcp::resolver resolver(ios);
    tcp::resolver::query query(host_name(), "");
    return resolver.resolve(query);
}

std::string NetworkUtils::hostIP(unsigned short family) {
    auto hostInetInfo = queryHostInetInfo();
    tcp::resolver::iterator end;
    while (hostInetInfo != end) {
        tcp::endpoint ep = *hostInetInfo++;
        sockaddr sa = *ep.data();
        if (sa.sa_family == family) {
            return ep.address().to_string();
        }
    }
    return nullptr;
}

}// namespace NES
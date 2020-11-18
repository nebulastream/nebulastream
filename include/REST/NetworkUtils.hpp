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

#pragma once

#include "std_service.hpp"
#include <string>

using namespace boost::asio;
using namespace boost::asio::ip;

namespace NES {

using HostInetInfo = tcp::resolver::iterator;

class NetworkUtils {
  private:
    static HostInetInfo queryHostInetInfo();
    static std::string hostIP(unsigned short family);

  public:
    // gets the host IP4 string formatted
    static std::string hostIP4() { return hostIP(AF_INET); }

    // gets the host IP6 string formatted
    static std::string hostIP6() { return hostIP(AF_INET6); }
    static std::string hostName() { return ip::host_name(); }
};

}// namespace NES

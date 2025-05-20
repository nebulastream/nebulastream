/*
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

#include <Configurations/Validation/IpValidation.hpp>

#include <cstddef>
#include <regex>
#include <string>

namespace NES::Configurations
{
bool IpValidation::isValid(const std::string& ip) const
{
    /// Accept localhost configuration
    if (ip == "localhost")
    {
        return true;
    }

    /// IPv4 address
    std::regex ipRegex("^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$");
    if (!std::regex_match(ip, ipRegex))
    {
        return false;
    }

    /// Checking each octet to be between 0 and 255
    size_t start = 0;
    size_t end = ip.find('.');
    while (end != std::string::npos)
    {
        int octet = std::stoi(ip.substr(start, end - start));
        if (octet < 0 || octet > 255)
        {
            return false;
        }
        start = end + 1;
        end = ip.find('.', start);
    }

    /// Checking the last octet
    int lastOctet = std::stoi(ip.substr(start, end));
    return lastOctet >= 0 && lastOctet <= 255;
}
}

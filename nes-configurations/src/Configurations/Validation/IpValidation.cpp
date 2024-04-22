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
#include <sstream>

namespace NES::Configurations {
bool IpValidation::isValid(const std::string& ip) const {
    // Regular expression to match a valid IPv4 address pattern
    std::regex ipRegex("^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$");

    // Check if the IP is a plausible IP address
    std::regex plausibleIpRegex("^(\\d+\\.){3}\\d+$");// Checks if it consists of numbers and dots
    if (std::regex_match(ip, plausibleIpRegex)) {
        // If it looks like an IP, it must match the strict IP regex
        if (!std::regex_match(ip, ipRegex)) {
            throw std::invalid_argument("Invalid IP address format: " + ip);
        }
        return true;// It's a valid IPv4 address
    } else {
        // It doesn't look like an IP address format; assume it's a hostname
        return true;
    }
}
}// namespace NES::Configurations
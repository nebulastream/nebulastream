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
#pragma once

#include <cstdint>
#include <string>
#include <Configurations/Validation/ConfigurationValidation.hpp>

namespace NES
{

/// @brief This class implements validation for Address Endpoints. Valid addresses:
/// "127.0.0.1:8080", "0.0.0.0:9092", ":8080" (not for grpc), "localhost:8080", "[::]:8080", "[::1]:8080", "[2001:db8::1]:8080"
class EndpointValidation final : public ConfigurationValidation
{
public:
    /// GRPC does not allow addresses with empty host names. If the configuration is intended to configure a GRPC Bind address, use
    /// the GRPC endpoint type.
    enum EndpointType : uint8_t
    {
        GRPC,
        OTHER
    };

private:
    EndpointType type;

public:
    explicit EndpointValidation(EndpointType type) : type(type) { }

    explicit EndpointValidation() : type(OTHER) { }

    /// @brief Method to check the validity of an ip address
    /// @param ip ip address
    /// @return success if validated
    [[nodiscard]] bool isValid(const std::string& endpointString) const override;
};
}

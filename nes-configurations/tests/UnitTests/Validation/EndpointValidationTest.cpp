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

#include <string>
#include <Configurations/Validation/EndpointValidation.hpp>
#include <gtest/gtest.h>

GTEST_TEST(EndpointValidationTest, ValidEndpoint)
{
    auto assertValid = [](const std::string& addr) { EXPECT_TRUE(NES::EndpointValidation{}.isValid(addr)); };
    auto assertInvalid
        = [](const std::string& addr) { EXPECT_ANY_THROW([[maybe_unused]] auto unused = NES::EndpointValidation{}.isValid(addr)); };

    /// Valid addresses
    assertValid("127.0.0.1:8080");
    assertValid("0.0.0.0:9092");
    assertValid(":8080");
    assertValid("localhost:8080");
    assertValid("[::]:8080");
    assertValid("1000.1234.423.2:8080"); /// Although this is not a valid ipv4 address it is still a valid hostname
    assertValid("[::1]:8080");
    assertValid("[2001:db8::1]:8080");

    /// Invalid addresses
    assertInvalid("127.0.0.1"); /// No port
    assertInvalid("127.0.0.1:"); /// Empty port
    assertInvalid("127.0.0.1:99999"); /// Port out of range
    assertInvalid("127.0.0.1:abc"); /// Invalid port format
    assertInvalid("invalid:port"); /// Invalid hostname
    assertInvalid("[::1]"); /// No port in IPv6
    assertInvalid("[invalid]:8080"); /// Invalid IPv6
    assertInvalid(""); /// Empty address
}

GTEST_TEST(EndpointValidationTest, ValidEndpointForGrpc)
{
    auto assertValid = [](const std::string& addr) { EXPECT_TRUE(NES::EndpointValidation{NES::EndpointValidation::GRPC}.isValid(addr)); };
    auto assertInvalid = [](const std::string& addr)
    { EXPECT_ANY_THROW([[maybe_unused]] auto unused = NES::EndpointValidation{NES::EndpointValidation::GRPC}.isValid(addr)); };

    /// Valid addresses
    assertValid("127.0.0.1:8080");
    assertValid("0.0.0.0:9092");
    assertValid("localhost:8080");
    assertValid("[::]:8080");
    assertValid("[::1]:8080");
    assertValid("[2001:db8::1]:8080");

    /// Invalid addresses with specific error messages
    assertInvalid(":8080"); /// grpc does not allow binding to empty host
    assertInvalid("127.0.0.1"); /// No port
    assertInvalid("127.0.0.1:"); /// Empty port
    assertInvalid("127.0.0.1:99999"); /// Port out of range
    assertInvalid("127.0.0.1:abc"); /// Invalid port format
    assertInvalid("invalid:port"); /// Invalid hostname
    assertInvalid("[::1]"); /// No port in IPv6
    assertInvalid("[invalid]:8080"); /// Invalid IPv6
    assertInvalid(""); /// Empty address
}

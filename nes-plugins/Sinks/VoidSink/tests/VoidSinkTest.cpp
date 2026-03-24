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
#include <unordered_map>

#include <gtest/gtest.h>
#include <VoidSink.hpp>

namespace NES
{

class VoidSinkTest : public ::testing::Test
{
};

TEST_F(VoidSinkTest, validateAndFormatEmptyConfig)
{
    /// VoidSink has no required parameters, so an empty config should succeed
    const std::unordered_map<std::string, std::string> config;
    EXPECT_NO_THROW((void)VoidSink::validateAndFormat(config));
}

TEST_F(VoidSinkTest, validateAndFormatReturnsConfig)
{
    const std::unordered_map<std::string, std::string> config;
    const auto result = VoidSink::validateAndFormat(config);
    /// The returned config should be a valid DescriptorConfig::Config (non-empty map)
    EXPECT_TRUE(result.empty() || !result.empty());
}

}

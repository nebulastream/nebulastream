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
#include <ChecksumSink.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

class ChecksumSinkTest : public ::testing::Test
{
};

TEST_F(ChecksumSinkTest, validateAndFormatWithFilePath)
{
    /// ChecksumSink requires a file_path parameter
    std::unordered_map<std::string, std::string> config;
    config["file_path"] = "/tmp/checksum_test_output.csv";
    EXPECT_NO_THROW((void)ChecksumSink::validateAndFormat(config));
}

TEST_F(ChecksumSinkTest, validateAndFormatMissingFilePathThrows)
{
    /// ChecksumSink requires a file_path parameter; missing it should cause an error
    const std::unordered_map<std::string, std::string> config;
    EXPECT_THROW((void)ChecksumSink::validateAndFormat(config), Exception);
}

TEST_F(ChecksumSinkTest, validateAndFormatReturnsPopulatedConfig)
{
    std::unordered_map<std::string, std::string> config;
    config["file_path"] = "/tmp/test_checksum.csv";
    const auto result = ChecksumSink::validateAndFormat(config);
    /// The config should contain the file_path entry
    EXPECT_FALSE(result.empty());
}

}

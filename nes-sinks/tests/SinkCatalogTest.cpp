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

#include <gtest/gtest.h>

#include <string>
#include <unordered_map>

#include <DataTypes/Schema.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

class SinkCatalogTest : public ::testing::Test
{
protected:
    SinkCatalog sinkCatalog;
    Schema schema;
    std::unordered_map<std::string, std::string> formatConfig;
};

/// Adding a valid sink descriptor should succeed.
TEST_F(SinkCatalogTest, addValidSinkDescriptor)
{
    auto result
        = sinkCatalog.addSinkDescriptor("mySink", schema, "File", {{"file_path", "/tmp/test.csv"}, {"output_format", "CSV"}}, formatConfig);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().getSinkName(), "mySink");
}

/// Sink names that consist only of digits are reserved and should be rejected.
TEST_F(SinkCatalogTest, rejectDigitOnlySinkName)
{
    auto result
        = sinkCatalog.addSinkDescriptor("123", schema, "File", {{"file_path", "/tmp/test.csv"}, {"output_format", "CSV"}}, formatConfig);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::InvalidConfigParameter);
}

/// An unknown sink type should fail validation and return an error.
TEST_F(SinkCatalogTest, rejectUnknownSinkType)
{
    auto result = sinkCatalog.addSinkDescriptor("mySink", schema, "NonExistentSinkType", {}, formatConfig);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::InvalidConfigParameter);
}

/// Adding a sink with the same name twice should overwrite (insert_or_assign behavior).
TEST_F(SinkCatalogTest, duplicateSinkNameOverwrites)
{
    auto result1 = sinkCatalog.addSinkDescriptor(
        "mySink", schema, "File", {{"file_path", "/tmp/test1.csv"}, {"output_format", "CSV"}}, formatConfig);
    ASSERT_TRUE(result1.has_value());

    auto result2 = sinkCatalog.addSinkDescriptor(
        "mySink", schema, "File", {{"file_path", "/tmp/test2.csv"}, {"output_format", "CSV"}}, formatConfig);
    ASSERT_TRUE(result2.has_value());

    auto descriptor = sinkCatalog.getSinkDescriptor("MYSINK");
    ASSERT_TRUE(descriptor.has_value());
}

}

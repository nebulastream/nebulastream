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

#include <Validator/ConfigMetadata.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace NES
{

class ConfigMetadataTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("ConfigMetadataTest.log", LogLevel::LOG_DEBUG);
    }

    void SetUp() override { BaseUnitTest::SetUp(); }
};

TEST_F(ConfigMetadataTest, GetRegisteredSourceTypesReturnsNonEmptyJson)
{
    auto json = Validator::getRegisteredSourceTypes();
    EXPECT_THAT(json, ::testing::StartsWith("["));
    EXPECT_THAT(json, ::testing::EndsWith("]"));
    // Should contain at least File and Generator
    EXPECT_THAT(json, ::testing::HasSubstr("\"FILE\""));
    EXPECT_THAT(json, ::testing::HasSubstr("\"GENERATOR\""));
}

TEST_F(ConfigMetadataTest, GetRegisteredSinkTypesReturnsNonEmptyJson)
{
    auto json = Validator::getRegisteredSinkTypes();
    EXPECT_THAT(json, ::testing::StartsWith("["));
    EXPECT_THAT(json, ::testing::EndsWith("]"));
    EXPECT_THAT(json, ::testing::HasSubstr("\"FILE\""));
    EXPECT_THAT(json, ::testing::HasSubstr("\"PRINT\""));
}

TEST_F(ConfigMetadataTest, GetSourceConfigFieldsForFileContainsFilePath)
{
    auto json = Validator::getSourceConfigFields("File");
    EXPECT_THAT(json, ::testing::StartsWith("["));
    EXPECT_THAT(json, ::testing::EndsWith("]"));
    EXPECT_THAT(json, ::testing::HasSubstr("\"file_path\""));
    EXPECT_THAT(json, ::testing::HasSubstr("\"required\":true"));
    EXPECT_THAT(json, ::testing::HasSubstr("\"type\":\"string\""));
}

TEST_F(ConfigMetadataTest, GetSourceConfigFieldsForGeneratorContainsSeed)
{
    auto json = Validator::getSourceConfigFields("Generator");
    EXPECT_THAT(json, ::testing::HasSubstr("\"seed\""));
    EXPECT_THAT(json, ::testing::HasSubstr("\"generator_schema\""));
    EXPECT_THAT(json, ::testing::HasSubstr("\"type\":\"number\""));
}

TEST_F(ConfigMetadataTest, GetSinkConfigFieldsForFileContainsFilePath)
{
    auto json = Validator::getSinkConfigFields("File");
    EXPECT_THAT(json, ::testing::HasSubstr("\"file_path\""));
    EXPECT_THAT(json, ::testing::HasSubstr("\"append\""));
    EXPECT_THAT(json, ::testing::HasSubstr("\"type\":\"boolean\""));
}

TEST_F(ConfigMetadataTest, GetSinkConfigFieldsForPrintContainsIngestion)
{
    auto json = Validator::getSinkConfigFields("Print");
    EXPECT_THAT(json, ::testing::HasSubstr("\"ingestion\""));
    EXPECT_THAT(json, ::testing::HasSubstr("\"type\":\"number\""));
}

TEST_F(ConfigMetadataTest, GetSinkConfigFieldsForVoidReturnsEmptyArray)
{
    auto json = Validator::getSinkConfigFields("Void");
    EXPECT_EQ(json, "[]");
}

TEST_F(ConfigMetadataTest, GetSourceConfigFieldsForUnknownTypeReturnsEmptyArray)
{
    auto json = Validator::getSourceConfigFields("NonExistent");
    EXPECT_EQ(json, "[]");
}

TEST_F(ConfigMetadataTest, CaseInsensitiveLookupWorks)
{
    auto jsonLower = Validator::getSourceConfigFields("file");
    auto jsonUpper = Validator::getSourceConfigFields("FILE");
    auto jsonMixed = Validator::getSourceConfigFields("File");
    EXPECT_EQ(jsonLower, jsonUpper);
    EXPECT_EQ(jsonLower, jsonMixed);
}

}

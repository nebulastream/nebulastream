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

#include <cstddef>
#include <string>
#include <unordered_map>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
class SinkCatalogTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("SinkCatalogTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SinkCatalog test class.");
    }

    static Schema<UnqualifiedUnboundField, Ordered> createTestSchema()
    {
        return Schema<UnqualifiedUnboundField, Ordered>{
            UnqualifiedUnboundField{Identifier::parse("stringField"), DataType::Type::VARSIZED},
            UnqualifiedUnboundField{Identifier::parse("intField"), DataType::Type::INT32}};
    }

    static std::unordered_map<Identifier, std::string> createValidFileSinkConfig()
    {
        return {{Identifier::parse("file_path"), "/dev/null"}, {Identifier::parse("output_format"), "CSV"}};
    }
};

TEST_F(SinkCatalogTest, AddInspectSinkDescriptor)
{
    auto sinkCatalog = SinkCatalog{};
    const auto sink = sinkCatalog.addSinkDescriptor(
        Identifier::parse("testSink"), createTestSchema(), Identifier::parse("File"), Host{"localhost"}, createValidFileSinkConfig(), {});
    ASSERT_TRUE(sink.has_value());
    ASSERT_TRUE(sinkCatalog.containsSinkDescriptor(sink.value()));
    ASSERT_TRUE(sinkCatalog.getSinkDescriptor(Identifier::parse("testSink")).has_value());
}

TEST_F(SinkCatalogTest, AddDuplicateSinkDescriptorReportsSinkAlreadyExists)
{
    auto sinkCatalog = SinkCatalog{};
    const auto first = sinkCatalog.addSinkDescriptor(
        Identifier::parse("testSink"), createTestSchema(), Identifier::parse("File"), Host{"localhost"}, createValidFileSinkConfig(), {});
    ASSERT_TRUE(first.has_value());

    const auto duplicate = sinkCatalog.addSinkDescriptor(
        Identifier::parse("testSink"), createTestSchema(), Identifier::parse("File"), Host{"localhost"}, createValidFileSinkConfig(), {});
    ASSERT_FALSE(duplicate.has_value());
    EXPECT_EQ(duplicate.error().code(), ErrorCode::SinkAlreadyExists);

    /// The original registration must remain untouched.
    const auto retained = sinkCatalog.getSinkDescriptor(Identifier::parse("testSink"));
    ASSERT_TRUE(retained.has_value());
    EXPECT_EQ(*retained, first.value());
    EXPECT_EQ(sinkCatalog.getAllSinkDescriptors().size(), size_t{1});
}

TEST_F(SinkCatalogTest, AddSinkDescriptorWithInvalidConfigReportsValidationError)
{
    auto sinkCatalog = SinkCatalog{};
    /// The file sink requires a file_path, so an empty config must fail validation.
    const auto sink = sinkCatalog.addSinkDescriptor(
        Identifier::parse("testSink"), createTestSchema(), Identifier::parse("File"), Host{"localhost"}, {}, {});
    ASSERT_FALSE(sink.has_value());
    EXPECT_EQ(sink.error().code(), ErrorCode::InvalidConfigParameter);
    EXPECT_FALSE(sinkCatalog.containsSinkDescriptor(Identifier::parse("testSink")));
}

TEST_F(SinkCatalogTest, AddSinkDescriptorWithUnknownSinkTypeReportsUnknownSinkType)
{
    auto sinkCatalog = SinkCatalog{};
    const auto sink = sinkCatalog.addSinkDescriptor(
        Identifier::parse("testSink"), createTestSchema(), Identifier::parse("THIS_DOES_NOT_EXIST"), Host{"localhost"}, {}, {});
    ASSERT_FALSE(sink.has_value());
    EXPECT_EQ(sink.error().code(), ErrorCode::UnknownSinkType);
    EXPECT_FALSE(sinkCatalog.containsSinkDescriptor(Identifier::parse("testSink")));
}

TEST_F(SinkCatalogTest, AddSinkDescriptorWithReservedNumericNameReportsInvalidConfigParameter)
{
    auto sinkCatalog = SinkCatalog{};
    const auto sink = sinkCatalog.addSinkDescriptor(
        Identifier::parse("12345"), createTestSchema(), Identifier::parse("File"), Host{"localhost"}, createValidFileSinkConfig(), {});
    ASSERT_FALSE(sink.has_value());
    EXPECT_EQ(sink.error().code(), ErrorCode::InvalidConfigParameter);
    EXPECT_FALSE(sinkCatalog.containsSinkDescriptor(Identifier::parse("12345")));
}
}

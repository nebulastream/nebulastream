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

#include <SourceCatalogs/SourceCatalog.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>

#include <DataTypes/DataType.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Ranges.hpp>
#include <gtest/gtest.h>

#include <BaseUnitTest.hpp>

class SourceCatalogTest : public NES::Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        NES::Logger::setupLogging("SourceCatalogTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SourceCatalog test class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }
};

TEST_F(SourceCatalogTest, AddInspectLogicalSource)
{
    auto sourceCatalog = NES::Catalogs::Source::SourceCatalog{};
    auto schema = NES::Schema{};
    schema.addField("stringField", NES::DataType::Type::VARSIZED);
    schema.addField("intField", NES::DataType::Type::INT32);

    const auto* const sourceName = "testSource";

    const auto sourceOpt = sourceCatalog.addLogicalSource(sourceName, schema);
    ASSERT_TRUE(sourceOpt.has_value());
    auto& source = *sourceOpt;

    ASSERT_TRUE(sourceCatalog.containsLogicalSource(source));
}


TEST_F(SourceCatalogTest, AddRemovePhysicalSources)
{
    auto sourceCatalog = NES::Catalogs::Source::SourceCatalog{};
    auto schema = NES::Schema{};
    schema.addField("stringField", NES::DataType::Type::VARSIZED);
    schema.addField("intField", NES::DataType::Type::INT32);

    const auto* const sourceName = "testSource";

    const auto sourceOpt = sourceCatalog.addLogicalSource(sourceName, schema);
    const auto logicalSource = *sourceOpt;
    const auto physical1 = *sourceCatalog.createPhysicalSource(
        NES::Configurations::DescriptorConfig::Config{},
        logicalSource,
        NES::INITIAL<NES::WorkerId>,
        "testSource1",
        NES::Sources::ParserConfig{});
    const auto physical2 = *sourceCatalog.createPhysicalSource(
        NES::Configurations::DescriptorConfig::Config{},
        logicalSource,
        NES::INITIAL<NES::WorkerId>,
        "testSource2",
        NES::Sources::ParserConfig{});

    ASSERT_EQ(physical1.getPhysicalSourceID(), 1);
    ASSERT_EQ(physical2.getPhysicalSourceID(), 2);

    auto expectedSources = std::array{physical1, physical2} | NES::ranges::to<std::set>();
    ASSERT_THAT(sourceCatalog.getPhysicalSources(logicalSource), testing::ContainerEq(expectedSources));

    ASSERT_TRUE(sourceCatalog.removePhysicalSource(physical1));

    const auto physical3 = *sourceCatalog.createPhysicalSource(
        NES::Configurations::DescriptorConfig::Config{},
        logicalSource,
        NES::INITIAL<NES::WorkerId>,
        "testSource3",
        NES::Sources::ParserConfig{});

    ASSERT_EQ(physical3.getPhysicalSourceID(), 3);

    const auto actualPhysicalSources = sourceCatalog.getPhysicalSources(logicalSource);

    expectedSources = std::array{physical2, physical3} | NES::ranges::to<std::set>();
    ASSERT_THAT(sourceCatalog.getPhysicalSources(logicalSource), expectedSources);
}

TEST_F(SourceCatalogTest, RemoveLogicalSource)
{
    auto sourceCatalog = NES::Catalogs::Source::SourceCatalog{};
    auto schema = NES::Schema{};
    schema.addField("stringField", NES::DataType::Type::VARSIZED);
    schema.addField("intField", NES::DataType::Type::INT32);

    const auto* const sourceName = "testSource";

    const auto sourceOpt = sourceCatalog.addLogicalSource(sourceName, schema);
    const auto logicalSource = *sourceOpt;
    const auto physical1 = *sourceCatalog.createPhysicalSource(
        NES::Configurations::DescriptorConfig::Config{},
        logicalSource,
        NES::INITIAL<NES::WorkerId>,
        "testSource1",
        NES::Sources::ParserConfig{});
    const auto physical2 = *sourceCatalog.createPhysicalSource(
        NES::Configurations::DescriptorConfig::Config{},
        logicalSource,
        NES::INITIAL<NES::WorkerId>,
        "testSource2",
        NES::Sources::ParserConfig{});

    auto expectedSources = std::array{physical1, physical2} | NES::ranges::to<std::set>();
    ASSERT_THAT(sourceCatalog.getPhysicalSources(logicalSource), testing::ContainerEq(expectedSources));

    ASSERT_TRUE(sourceCatalog.removeLogicalSource(logicalSource));

    ASSERT_FALSE(sourceCatalog.containsLogicalSource(logicalSource));
    ASSERT_FALSE(sourceCatalog.getPhysicalSource(physical1.getPhysicalSourceID()).has_value());
    ASSERT_FALSE(sourceCatalog.getPhysicalSource(physical2.getPhysicalSourceID()).has_value());

}

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

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <random>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <Codecs/DecoderCatalog.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/FileSourceConfig.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/Pointers.hpp>
#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
class SourceCatalogTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("SourceCatalogTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SourceCatalog test class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }
};

namespace
{
std::expected<SourceDescriptor, Exception>
addFilePhysicalSource(SharedPtr<SourceCatalog>& sourceCatalog, const LogicalSource& logicalSource, const Identifier& sourceType)
{
    const Schema<LiteralConfigValue, Ordered> values{
        LiteralConfigValue{QualifiedIdentifier::parse("file_path"), "/dev/null"},
        LiteralConfigValue{QualifiedIdentifier::parse("host"), "localhost"},
        LiteralConfigValue{QualifiedIdentifier::parse("type"), "CSV"}};
    auto configSchema = SourceCatalog::getConfigSchema(sourceType, Identifier::parse("CSV"));
    if (not configSchema.has_value())
    {
        return std::unexpected{configSchema.error()};
    }
    auto resolved = configSchema->resolveConfigs(values);
    if (not resolved.has_value())
    {
        return std::unexpected{resolved.error()};
    }
    auto [generalConfig, pluginConfig, inputFormatterDescriptor, declaredSchema] = std::move(resolved).value();
    return sourceCatalog->registerWithLogicalSource(
        PhysicalSourceBuilder{std::move(generalConfig), std::move(pluginConfig), std::move(inputFormatterDescriptor)},
        logicalSource.getLogicalSourceName());
}
}

/// clang tidy doesn't recognize the .has_value in the ASSERT_TRUE
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
TEST_F(SourceCatalogTest, AddInspectLogicalSource)
{
    auto sourceCatalog = SourceCatalog::create();
    const auto schema = Schema<UnqualifiedUnboundField, Ordered>{
        UnqualifiedUnboundField{Identifier::parse("stringField"), DataType::Type::VARSIZED},
        UnqualifiedUnboundField{Identifier::parse("intField"), DataType::Type::INT32}};

    const auto sourceOpt = sourceCatalog->addLogicalSource(Identifier::parse("testSource"), schema);
    ASSERT_TRUE(sourceOpt.has_value());
    ASSERT_TRUE(sourceCatalog->containsLogicalSource(*sourceOpt));
}

TEST_F(SourceCatalogTest, ConfigMapsRequireExplicitQuotingForNumericStrings)
{
    SourceCatalog catalog;
    const auto schema
        = Schema<UnqualifiedUnboundField, Ordered>{UnqualifiedUnboundField{Identifier::parse("value"), DataType::Type::INT32}};
    const auto logical = catalog.addLogicalSource(Identifier::parse("input"), schema);
    ASSERT_TRUE(logical.has_value());
    EXPECT_FALSE(catalog
                     .addPhysicalSource(
                         *logical,
                         Identifier::parse("FILE"),
                         Host{"localhost:8080"},
                         {{Identifier::parse("FILE_PATH"), "123"}},
                         {{Identifier::parse("TYPE"), "CSV"}})
                     .has_value());
    EXPECT_FALSE(catalog
                     .addPhysicalSource(
                         *logical,
                         Identifier::parse("FILE"),
                         Host{"localhost:8080"},
                         {{Identifier::parse("FILE_PATH"), "'123'"}},
                         {{Identifier::parse("TYPE"), "CSV"}, {Identifier::parse("FIELD_DELIMITER"), "1"}})
                     .has_value());
    const auto source = catalog.addPhysicalSource(
        *logical,
        Identifier::parse("FILE"),
        Host{"localhost:8080"},
        {{Identifier::parse("FILE_PATH"), "'123'"}, {Identifier::parse("MAX_INFLIGHT_BUFFERS"), "2"}},
        {{Identifier::parse("TYPE"), "CSV"}, {Identifier::parse("FIELD_DELIMITER"), "'1'"}});
    ASSERT_TRUE(source.has_value()) << source.error();
    EXPECT_EQ(source->getPluginData().getAs<FileSourceConfig>().filePath, "123");
    EXPECT_EQ(source->getMaxInflightBuffers(), 2);
}

namespace
{
std::expected<SourceDescriptor, Exception>
addFileSource(SourceCatalog& catalog, const LogicalSource& logical, const std::optional<std::string>& decoderNames)
{
    std::unordered_map<Identifier, std::string> config{{Identifier::parse("FILE_PATH"), "'/dev/null'"}};
    if (decoderNames)
    {
        config.emplace(Identifier::parse("DECODERS"), fmt::format("'{}'", *decoderNames));
    }
    return catalog.addPhysicalSource(
        logical, Identifier::parse("FILE"), Host{"localhost:8080"}, config, {{Identifier::parse("TYPE"), "CSV"}});
}

LogicalSource addLogicalSource(SourceCatalog& catalog)
{
    const auto logical = catalog.addLogicalSource(
        Identifier::parse("input"),
        Schema<UnqualifiedUnboundField, Ordered>{UnqualifiedUnboundField{Identifier::parse("value"), DataType::Type::INT32}});
    if (not logical.has_value())
    {
        throw TestException("Cannot create the logical source");
    }
    return *logical;
}
}

TEST_F(SourceCatalogTest, DecoderReferencesWithoutCodec)
{
    auto decoders = std::make_shared<DecoderCatalog>();
    const auto unknownCodec = decoders->addDecoder(Identifier::parse("other"), Identifier::parse("NOT_A_CODEC"), {});
    ASSERT_FALSE(unknownCodec.has_value());
    EXPECT_EQ(unknownCodec.error().code(), ErrorCode::UnknownDecoderType) << unknownCodec.error().what();
    EXPECT_THAT(unknownCodec.error().what(), ::testing::HasSubstr("NOT_A_CODEC"));
    EXPECT_FALSE(decoders->removeDecoder(Identifier::parse("missing")).has_value());

    SourceCatalog catalog{decoders};
    const auto logical = addLogicalSource(catalog);
    const auto withUnknownDecoder = addFileSource(catalog, logical, "missing");
    ASSERT_FALSE(withUnknownDecoder.has_value());
    EXPECT_EQ(withUnknownDecoder.error().code(), ErrorCode::UnknownDecoderName) << withUnknownDecoder.error().what();
    EXPECT_THAT(withUnknownDecoder.error().what(), ::testing::HasSubstr("MISSING"));

    const auto withoutDecoder = addFileSource(catalog, logical, std::nullopt);
    ASSERT_TRUE(withoutDecoder.has_value()) << withoutDecoder.error();
    EXPECT_TRUE(withoutDecoder->getDecoders().empty());
}

TEST_F(SourceCatalogTest, DecoderCatalogValidatesDecoders)
{
    DecoderCatalog decoders;
    const auto created = decoders.addDecoder(Identifier::parse("compressed"), Identifier::parse("ZSTD"), {});
    ASSERT_TRUE(created.has_value()) << created.error();
    EXPECT_EQ(created->getName(), Identifier::parse("compressed"));
    EXPECT_EQ(created->getType(), Identifier::parse("ZSTD"));

    const auto duplicate = decoders.addDecoder(Identifier::parse("compressed"), Identifier::parse("ZSTD"), {});
    ASSERT_FALSE(duplicate.has_value());
    EXPECT_EQ(duplicate.error().code(), ErrorCode::DecoderAlreadyExists) << duplicate.error().what();

    const auto unknownOption
        = decoders.addDecoder(Identifier::parse("withOption"), Identifier::parse("ZSTD"), {{Identifier::parse("LEVEL"), "3"}});
    ASSERT_FALSE(unknownOption.has_value());
    EXPECT_EQ(unknownOption.error().code(), ErrorCode::InvalidConfigParameter) << unknownOption.error().what();

    EXPECT_EQ(decoders.getAllDecoders().size(), 1);
    EXPECT_TRUE(decoders.removeDecoder(Identifier::parse("compressed")).has_value());
    EXPECT_FALSE(decoders.getDecoder(Identifier::parse("compressed")).has_value());
}

TEST_F(SourceCatalogTest, SourceKeepsResolvedDecoder)
{
    auto decoders = std::make_shared<DecoderCatalog>();
    ASSERT_TRUE(decoders->addDecoder(Identifier::parse("compressed"), Identifier::parse("ZSTD"), {}).has_value());
    SourceCatalog catalog{decoders};
    const auto logical = addLogicalSource(catalog);

    const auto withDecoder = addFileSource(catalog, logical, "compressed");
    ASSERT_TRUE(withDecoder.has_value()) << withDecoder.error();
    ASSERT_EQ(withDecoder->getDecoders().size(), 1);
    EXPECT_EQ(withDecoder->getDecoders().front().getType(), Identifier::parse("ZSTD"));

    /// Decoder chains follow with #2188.
    const auto withChain = addFileSource(catalog, logical, "compressed, compressed");
    ASSERT_FALSE(withChain.has_value());
    EXPECT_EQ(withChain.error().code(), ErrorCode::InvalidConfigParameter) << withChain.error().what();

    ASSERT_TRUE(decoders->removeDecoder(Identifier::parse("compressed")).has_value());
    const auto stored = catalog.getPhysicalSource(withDecoder->getPhysicalSourceId());
    ASSERT_TRUE(stored.has_value());
    EXPECT_EQ(stored->getDecoders().size(), 1);
}

TEST_F(SourceCatalogTest, AddRemovePhysicalSources)
{
    auto sourceCatalog = SourceCatalog::create();
    auto schema = Schema<UnqualifiedUnboundField, Ordered>{
        UnqualifiedUnboundField{Identifier::parse("stringField"), DataType::Type::VARSIZED},
        UnqualifiedUnboundField{Identifier::parse("intField"), DataType::Type::INT32}};

    const auto sourceOpt = sourceCatalog->addLogicalSource(Identifier::parse("testSource"), schema);
    ASSERT_TRUE(sourceOpt.has_value());
    const auto physical1Opt = addFilePhysicalSource(sourceCatalog, *sourceOpt, Identifier::parse("File"));
    const auto physical2Opt = addFilePhysicalSource(sourceCatalog, *sourceOpt, Identifier::parse("File"));

    ASSERT_TRUE(physical1Opt.has_value());
    ASSERT_TRUE(physical2Opt.has_value());
    const auto& physical1 = physical1Opt.value();
    const auto& physical2 = physical2Opt.value();

    ASSERT_EQ(physical1.getPhysicalSourceId(), PhysicalSourceId{INITIAL_PHYSICAL_SOURCE_ID.getRawValue()});
    ASSERT_EQ(physical2.getPhysicalSourceId(), PhysicalSourceId{INITIAL_PHYSICAL_SOURCE_ID.getRawValue() + 1});

    ASSERT_TRUE(sourceCatalog->getPhysicalSource(physical1.getPhysicalSourceId()).has_value());
    ASSERT_TRUE(sourceCatalog->getPhysicalSource(physical2.getPhysicalSourceId()).has_value());
    ASSERT_EQ(sourceCatalog->getPhysicalSource(physical1.getPhysicalSourceId()).value(), physical1);
    ASSERT_EQ(sourceCatalog->getPhysicalSource(physical2.getPhysicalSourceId()).value(), physical2);

    auto expectedSources = std::unordered_set{physical1, physical2};
    const auto expect12Opt = sourceCatalog->getPhysicalSources(*sourceOpt);
    ASSERT_TRUE(expect12Opt.has_value());
    ASSERT_THAT(expect12Opt.value(), testing::ContainerEq(expectedSources));

    ASSERT_TRUE(sourceCatalog->removePhysicalSource(physical1));

    const auto physical3Opt = addFilePhysicalSource(sourceCatalog, *sourceOpt, Identifier::parse("File"));
    ASSERT_TRUE(physical2Opt.has_value());
    const auto& physical3 = physical3Opt.value();

    ASSERT_EQ(physical3.getPhysicalSourceId(), PhysicalSourceId{INITIAL_PHYSICAL_SOURCE_ID.getRawValue() + 2});
    ASSERT_TRUE(sourceCatalog->getPhysicalSource(physical3.getPhysicalSourceId()).has_value());
    ASSERT_EQ(sourceCatalog->getPhysicalSource(physical3.getPhysicalSourceId()).value(), physical3);

    const auto actualPhysicalSources = sourceCatalog->getPhysicalSources(*sourceOpt);

    expectedSources = std::unordered_set{physical2, physical3};
    ASSERT_THAT(sourceCatalog->getPhysicalSources(*sourceOpt), expectedSources);
}

TEST_F(SourceCatalogTest, AddInvalidPhysicalSource)
{
    auto sourceCatalog = SourceCatalog::create();
    auto schema = Schema<UnqualifiedUnboundField, Ordered>{
        UnqualifiedUnboundField{Identifier::parse("stringField"), DataType::Type::VARSIZED},
        UnqualifiedUnboundField{Identifier::parse("intField"), DataType::Type::INT32}};

    const auto sourceOpt = sourceCatalog->addLogicalSource(Identifier::parse("testSource"), schema);
    ASSERT_TRUE(sourceOpt.has_value());
    const auto physical1Opt = addFilePhysicalSource(sourceCatalog, *sourceOpt, Identifier::parse("THIS_DOES_NOT_EXIST"));
    ASSERT_FALSE(physical1Opt.has_value());
}

TEST_F(SourceCatalogTest, RemoveLogicalSource)
{
    auto sourceCatalog = SourceCatalog::create();
    auto schema = Schema<UnqualifiedUnboundField, Ordered>{
        UnqualifiedUnboundField{Identifier::parse("stringField"), DataType::Type::VARSIZED},
        UnqualifiedUnboundField{Identifier::parse("intField"), DataType::Type::INT32}};

    const auto sourceOpt = sourceCatalog->addLogicalSource(Identifier::parse("testSource"), schema);
    ASSERT_TRUE(sourceOpt.has_value());
    const auto& logicalSource = sourceOpt.value();
    const auto physical1Opt = addFilePhysicalSource(sourceCatalog, logicalSource, Identifier::parse("File"));
    const auto physical2Opt = addFilePhysicalSource(sourceCatalog, logicalSource, Identifier::parse("File"));

    ASSERT_TRUE(physical1Opt.has_value());
    ASSERT_TRUE(physical2Opt.has_value());
    const auto& physical1 = physical1Opt.value();
    const auto& physical2 = physical2Opt.value();

    auto expectedSources = std::unordered_set{physical1, physical2};

    const auto actualSourcesOpt = sourceCatalog->getPhysicalSources(logicalSource);
    ASSERT_TRUE(actualSourcesOpt.has_value());
    ASSERT_THAT(actualSourcesOpt.value(), testing::ContainerEq(expectedSources));

    ASSERT_TRUE(sourceCatalog->removeLogicalSource(logicalSource));

    ASSERT_FALSE(sourceCatalog->containsLogicalSource(logicalSource));
    ASSERT_FALSE(sourceCatalog->getPhysicalSource(physical1.getPhysicalSourceId()).has_value());
    ASSERT_FALSE(sourceCatalog->getPhysicalSource(physical2.getPhysicalSourceId()).has_value());
}

TEST_F(SourceCatalogTest, ConcurrentSourceCatalogModification)
{
    /// The source catalog might get accessed concurrently, depending on the frontend it is behind.
    /// The current implementation has a simple lock, future implemenations might have some MVCC mechanism.
    /// This test exists mostly to test the source catalog with the thread sanitizer
    constexpr size_t numPhysicalAddThreads = 10;
    constexpr size_t operationsPerThread = 1000;
    constexpr unsigned int concurrentLogicalSourceNames = 3;
    auto sourceCatalog = SourceCatalog::create();
    auto schema = Schema<UnqualifiedUnboundField, Ordered>{
        UnqualifiedUnboundField{Identifier::parse("stringField"), DataType::Type::VARSIZED},
        UnqualifiedUnboundField{Identifier::parse("intField"), DataType::Type::INT32}};

    std::atomic_uint64_t successfulPhysicalAdds{0};
    std::atomic_uint64_t failedPhysicalAdds{0};
    std::atomic_uint64_t failedLogicalAdds{0};

    std::array<std::jthread, numPhysicalAddThreads> threads{};

    std::array<std::atomic_flag, numPhysicalAddThreads> threadsDone{};


    std::stop_source stopSource; /// NOLINT(misc-const-correctness)
    auto physicalAddThreadFunction = [&, stopToken = stopSource.get_token()](const int threadNum)
    {
        std::random_device device;
        std::mt19937 gen(device());
        std::uniform_int_distribution<> range(0, concurrentLogicalSourceNames - 1);
        std::array<int, operationsPerThread> nums{};
        std::ranges::generate(nums, [&]() { return range(gen); });
        for (int num : nums)
        {
            auto logicalSourceName = Identifier::parse(fmt::format("testSource{}", num));

            auto logicalSourceOpt = sourceCatalog->getLogicalSource(logicalSourceName);
            if (not logicalSourceOpt.has_value())
            {
                logicalSourceOpt = sourceCatalog->addLogicalSource(logicalSourceName, schema);
            }
            if (logicalSourceOpt.has_value())
            {
                auto physicalSourceOpt = addFilePhysicalSource(sourceCatalog, *logicalSourceOpt, Identifier::parse("File"));
                if (physicalSourceOpt.has_value())
                {
                    successfulPhysicalAdds.fetch_add(1);
                }
                else
                {
                    failedPhysicalAdds.fetch_add(1);
                }
            }
            else
            {
                failedLogicalAdds.fetch_add(1);
            }
        }
        threadsDone.at(threadNum).test_and_set();
        threadsDone.at(threadNum).notify_all();
        while (not stopToken.stop_requested())
        {
            constexpr auto waitFor = std::chrono::milliseconds(10);
            std::this_thread::sleep_for(waitFor);
        }
    };

    auto logicalRemoveThreadFunction = [&, stopToken = stopSource.get_token()]
    {
        std::random_device device;
        std::mt19937 gen(device());
        std::uniform_int_distribution<> range(0, concurrentLogicalSourceNames - 1);
        while (not stopToken.stop_requested())
        {
            int num = range(gen);
            auto logicalSourceName = Identifier::parse(fmt::format("testSource{}", num));
            if (auto logicalSourceOpt = sourceCatalog->getLogicalSource(logicalSourceName))
            {
                const bool unused = sourceCatalog->removeLogicalSource(*logicalSourceOpt);
            }
        }
    };

    const std::jthread removalThread{logicalRemoveThreadFunction};
    for (uint32_t i = 0; i < numPhysicalAddThreads; ++i)
    {
        threads.at(i) = std::jthread{physicalAddThreadFunction, i};
    }

    for (auto& done : threadsDone)
    {
        done.wait(false);
    }

    const auto stopped = stopSource.request_stop();
    ASSERT_TRUE(stopped);

    ASSERT_EQ(successfulPhysicalAdds + failedPhysicalAdds + failedLogicalAdds, numPhysicalAddThreads * operationsPerThread);
    NES_INFO(
        "Added {} physical sources successfully, and {} failed, while {} logical source adds failed",
        successfulPhysicalAdds,
        failedPhysicalAdds,
        failedLogicalAdds);
}
}

/// NOLINTEND(bugprone-unchecked-optional-access)

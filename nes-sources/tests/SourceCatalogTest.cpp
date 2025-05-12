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

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <random>
#include <stop_token>
#include <thread>
#include <unordered_set>

#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include "Common/DataTypes/DataTypeProvider.hpp"

#include <API/Schema.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include <BaseUnitTest.hpp>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <gmock/gmock.h>

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
    schema.addField("stringField", NES::DataTypeProvider::provideDataType(NES::LogicalType::VARSIZED));
    schema.addField("intField", NES::DataTypeProvider::provideBasicType(NES::BasicType::INT32));

    const auto* const sourceName = "testSource";

    const auto sourceOpt = sourceCatalog.addLogicalSource(sourceName, schema);
    ASSERT_TRUE(sourceOpt.has_value());
    const auto& source = *sourceOpt;

    ASSERT_TRUE(sourceCatalog.containsLogicalSource(source));
}


TEST_F(SourceCatalogTest, AddRemovePhysicalSources)
{
    auto sourceCatalog = NES::Catalogs::Source::SourceCatalog{};
    auto schema = NES::Schema{};
    schema.addField("stringField", NES::DataTypeProvider::provideDataType(NES::LogicalType::VARSIZED));
    schema.addField("intField", NES::DataTypeProvider::provideBasicType(NES::BasicType::INT32));

    const auto* const sourceName = "testSource";

    const auto sourceOpt = sourceCatalog.addLogicalSource(sourceName, schema);
    ASSERT_TRUE(sourceOpt.has_value());
    const auto& logicalSource = sourceOpt.value();
    const auto physical1 = *sourceCatalog.addPhysicalSource(
        logicalSource,
        NES::INITIAL<NES::WorkerId>,
        "testSource1",
        NES::Sources::SourceDescriptor::INVALID_BUFFERS_IN_LOCAL_POOL,
        NES::Configurations::DescriptorConfig::Config{},
        NES::Sources::ParserConfig{});
    const auto physical2Opt = sourceCatalog.addPhysicalSource(
        logicalSource,
        NES::INITIAL<NES::WorkerId>,
        "testSource2",
        NES::Sources::SourceDescriptor::INVALID_BUFFERS_IN_LOCAL_POOL,
        NES::Configurations::DescriptorConfig::Config{},
        NES::Sources::ParserConfig{});
    ASSERT_TRUE(physical2Opt.has_value());
    const auto physical2 = physical2Opt.value();

    ASSERT_EQ(physical1.getPhysicalSourceId(), 0);
    ASSERT_EQ(physical2.getPhysicalSourceId(), 1);

    auto expectedSources = std::unordered_set<NES::Sources::SourceDescriptor>{};
    expectedSources.insert(physical1);
    expectedSources.insert(physical2);
    const auto expect12Opt = sourceCatalog.getPhysicalSources(logicalSource);
    ASSERT_TRUE(expect12Opt.has_value());
    ASSERT_THAT(expect12Opt.value(), testing::ContainerEq(expectedSources));

    ASSERT_TRUE(sourceCatalog.removePhysicalSource(physical1));

    const auto physical3Opt = sourceCatalog.addPhysicalSource(
        logicalSource,
        NES::INITIAL<NES::WorkerId>,
        "testSource3",
        NES::Sources::SourceDescriptor::INVALID_BUFFERS_IN_LOCAL_POOL,
        NES::Configurations::DescriptorConfig::Config{},
        NES::Sources::ParserConfig{});
    ASSERT_TRUE(physical2Opt.has_value());
    const auto physical3 = physical3Opt.value();

    ASSERT_EQ(physical3.getPhysicalSourceId(), 2);

    const auto actualPhysicalSources = sourceCatalog.getPhysicalSources(logicalSource);

    expectedSources = std::unordered_set<NES::Sources::SourceDescriptor>{};
    expectedSources.insert(physical2);
    expectedSources.insert(physical3);
    ASSERT_THAT(sourceCatalog.getPhysicalSources(logicalSource), expectedSources);
}

TEST_F(SourceCatalogTest, RemoveLogicalSource)
{
    auto sourceCatalog = NES::Catalogs::Source::SourceCatalog{};
    auto schema = NES::Schema{};
    schema.addField("stringField", NES::DataTypeProvider::provideDataType(NES::LogicalType::VARSIZED));
    schema.addField("intField", NES::DataTypeProvider::provideBasicType(NES::BasicType::INT32));

    const auto* const sourceName = "testSource";

    const auto sourceOpt = sourceCatalog.addLogicalSource(sourceName, schema);
    ASSERT_TRUE(sourceOpt.has_value());
    const auto& logicalSource = sourceOpt.value();
    const auto physical1Opt = sourceCatalog.addPhysicalSource(
        logicalSource,
        NES::INITIAL<NES::WorkerId>,
        "testSource1",
        NES::Sources::SourceDescriptor::INVALID_BUFFERS_IN_LOCAL_POOL,
        NES::Configurations::DescriptorConfig::Config{},
        NES::Sources::ParserConfig{});
    const auto physical2Opt = sourceCatalog.addPhysicalSource(
        logicalSource,
        NES::INITIAL<NES::WorkerId>,
        "testSource2",
        NES::Sources::SourceDescriptor::INVALID_BUFFERS_IN_LOCAL_POOL,
        NES::Configurations::DescriptorConfig::Config{},
        NES::Sources::ParserConfig{});

    ASSERT_TRUE(physical1Opt.has_value());
    ASSERT_TRUE(physical2Opt.has_value());
    const auto physical1 = physical1Opt.value();
    const auto physical2 = physical2Opt.value();

    auto expectedSources = std::unordered_set<NES::Sources::SourceDescriptor>{};
    expectedSources.insert(physical1);
    expectedSources.insert(physical2);

    auto const actualSourcesOpt = sourceCatalog.getPhysicalSources(logicalSource);
    ASSERT_TRUE(actualSourcesOpt.has_value());
    ASSERT_THAT(actualSourcesOpt.value(), testing::ContainerEq(expectedSources));

    ASSERT_TRUE(sourceCatalog.removeLogicalSource(logicalSource));

    ASSERT_FALSE(sourceCatalog.containsLogicalSource(logicalSource));
    ASSERT_FALSE(sourceCatalog.getPhysicalSource(physical1.getPhysicalSourceId()).has_value());
    ASSERT_FALSE(sourceCatalog.getPhysicalSource(physical2.getPhysicalSourceId()).has_value());
}


TEST_F(SourceCatalogTest, ConcurrentSourceCatalogModification)
{
    /// Since the catalogs don't provide any kind of larger transaction guarantees, except for the atomicity of a single function call,
    /// This test exists mostly to test the source catalog with the thread sanitizer
    constexpr size_t numPhysicalAddThreads = 10;
    constexpr int perThread = 1000;
    constexpr unsigned int concurrentLogicalSourceNames = 3;
    auto sourceCatalog = NES::Catalogs::Source::SourceCatalog{};
    auto schema = NES::Schema{};
    schema.addField("stringField", NES::DataTypeProvider::provideDataType(NES::LogicalType::VARSIZED));
    schema.addField("intField", NES::DataTypeProvider::provideBasicType(NES::BasicType::INT32));

    std::atomic_uint64_t successfulPhysicalAdds{0};
    std::atomic_uint64_t failedPhysicalAdds{0};
    std::atomic_uint64_t failedLogicalAdds{0};

    std::array<std::thread, numPhysicalAddThreads> threads{};

    std::array<std::atomic_flag, numPhysicalAddThreads> threadsDone{};

    std::stop_source stopSource;
    auto physicalAddThreadFunction = [&, stopToken = stopSource.get_token()](const int threadNum)
    {
        std::random_device device;
        std::mt19937 gen(device());
        std::uniform_int_distribution<> range(0, concurrentLogicalSourceNames - 1);
        for (int i = 0; i < perThread; ++i)
        {
            int num = range(gen);
            auto logicalSourceName = fmt::format("testSource{}", num);

            auto logicalSourceOpt = sourceCatalog.getLogicalSource(logicalSourceName);
            if (not logicalSourceOpt.has_value())
            {
                logicalSourceOpt = sourceCatalog.addLogicalSource(logicalSourceName, schema);
            }
            if (logicalSourceOpt.has_value())
            {
                auto physicalSourceOpt = sourceCatalog.addPhysicalSource(
                    *logicalSourceOpt,
                    NES::INITIAL<NES::WorkerId>,
                    "testSource",
                    NES::Sources::SourceDescriptor::INVALID_BUFFERS_IN_LOCAL_POOL,
                    NES::Configurations::DescriptorConfig::Config{},
                    NES::Sources::ParserConfig{});
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
            auto logicalSourceName = fmt::format("testSource{}", num);
            if (auto logicalSourceOpt = sourceCatalog.getLogicalSource(logicalSourceName))
            {
                sourceCatalog.removeLogicalSource(*logicalSourceOpt);
            }
        }
    };

    std::thread removalThread{logicalRemoveThreadFunction};
    for (unsigned int i = 0; i < numPhysicalAddThreads; ++i)
    {
        threads.at(i) = std::thread{physicalAddThreadFunction, i};
    }

    for (auto& done : threadsDone)
    {
        done.wait(false);
    }

    auto stopped = stopSource.request_stop();
    ASSERT_TRUE(stopped);

    for (unsigned int i = 0; i < numPhysicalAddThreads; ++i)
    {
        threads.at(i).join();
    }
    removalThread.join();

    ASSERT_EQ(successfulPhysicalAdds + failedPhysicalAdds + failedLogicalAdds, numPhysicalAddThreads * perThread);
    NES_INFO(
        "Added {} physical sources successfully, and {} failed, while {} logical source adds failed",
        successfulPhysicalAdds,
        failedPhysicalAdds,
        failedLogicalAdds);
}

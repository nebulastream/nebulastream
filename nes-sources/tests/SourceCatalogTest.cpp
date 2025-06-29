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
#include <random>
#include <stop_token>
#include <thread>
#include <unordered_set>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES
{
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

/// clang tidy doesn't recognize the .has_value in the ASSERT_TRUE
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
TEST_F(SourceCatalogTest, AddInspectLogicalSource)
{
    auto sourceCatalog = SourceCatalog{};
    auto schema = Schema{};
    schema.addField("stringField", DataTypeProvider::provideDataType(DataType::Type::VARSIZED));
    schema.addField("intField", DataTypeProvider::provideDataType(DataType::Type::INT32));

    const auto sourceOpt = sourceCatalog.addLogicalSource("testSource", schema);
    ASSERT_TRUE(sourceOpt.has_value());
    ASSERT_TRUE(sourceCatalog.containsLogicalSource(*sourceOpt));
}


TEST_F(SourceCatalogTest, AddRemovePhysicalSources)
{
    auto sourceCatalog = SourceCatalog{};
    auto schema = Schema{};
    schema.addField("stringField", DataTypeProvider::provideDataType(DataType::Type::VARSIZED));
    schema.addField("intField", DataTypeProvider::provideDataType(DataType::Type::INT32));

    const auto sourceOpt = sourceCatalog.addLogicalSource("testSource", schema);
    ASSERT_TRUE(sourceOpt.has_value());
    const auto physical1 = *sourceCatalog.addPhysicalSource(
        *sourceOpt,
        NES::INITIAL<NES::WorkerId>,
        "testSource1",
        SourceDescriptor::INVALID_NUMBER_OF_BUFFERS_IN_LOCAL_POOL,
        Configurations::DescriptorConfig::Config{},
        ParserConfig{});
    const auto physical2Opt = sourceCatalog.addPhysicalSource(
        *sourceOpt,
        NES::INITIAL<NES::WorkerId>,
        "testSource2",
        SourceDescriptor::INVALID_NUMBER_OF_BUFFERS_IN_LOCAL_POOL,
        Configurations::DescriptorConfig::Config{},
        ParserConfig{});
    ASSERT_TRUE(physical2Opt.has_value());
    const auto& physical2 = physical2Opt.value();

    ASSERT_EQ(physical1.getPhysicalSourceId(), PhysicalSourceId{INITIAL_PHYSICAL_SOURCE_ID.getRawValue()});
    ASSERT_EQ(physical2.getPhysicalSourceId(), PhysicalSourceId{INITIAL_PHYSICAL_SOURCE_ID.getRawValue()+1});

    ASSERT_TRUE(sourceCatalog.getPhysicalSource(physical1.getPhysicalSourceId()).has_value());
    ASSERT_TRUE(sourceCatalog.getPhysicalSource(physical2.getPhysicalSourceId()).has_value());
    ASSERT_EQ(sourceCatalog.getPhysicalSource(physical1.getPhysicalSourceId()).value(), physical1);
    ASSERT_EQ(sourceCatalog.getPhysicalSource(physical2.getPhysicalSourceId()).value(), physical2);

    auto expectedSources = std::unordered_set{physical1, physical2};
    const auto expect12Opt = sourceCatalog.getPhysicalSources(*sourceOpt);
    ASSERT_TRUE(expect12Opt.has_value());
    ASSERT_THAT(expect12Opt.value(), testing::ContainerEq(expectedSources));

    ASSERT_TRUE(sourceCatalog.removePhysicalSource(physical1));

    const auto physical3Opt = sourceCatalog.addPhysicalSource(
        *sourceOpt,
        NES::INITIAL<WorkerId>,
        "testSource3",
        SourceDescriptor::INVALID_NUMBER_OF_BUFFERS_IN_LOCAL_POOL,
        Configurations::DescriptorConfig::Config{},
        ParserConfig{});
    ASSERT_TRUE(physical2Opt.has_value());
    const auto& physical3 = physical3Opt.value();

    ASSERT_EQ(physical3.getPhysicalSourceId(), PhysicalSourceId{INITIAL_PHYSICAL_SOURCE_ID.getRawValue()+2});
    ASSERT_TRUE(sourceCatalog.getPhysicalSource(physical3.getPhysicalSourceId()).has_value());
    ASSERT_EQ(sourceCatalog.getPhysicalSource(physical3.getPhysicalSourceId()).value(), physical3);

    const auto actualPhysicalSources = sourceCatalog.getPhysicalSources(*sourceOpt);

    expectedSources = std::unordered_set{physical2, physical3};
    ASSERT_THAT(sourceCatalog.getPhysicalSources(*sourceOpt), expectedSources);
}

TEST_F(SourceCatalogTest, RemoveLogicalSource)
{
    auto sourceCatalog = SourceCatalog{};
    auto schema = Schema{};
    schema.addField("stringField", DataTypeProvider::provideDataType(DataType::Type::VARSIZED));
    schema.addField("intField", DataTypeProvider::provideDataType(DataType::Type::INT32));

    const auto sourceOpt = sourceCatalog.addLogicalSource("testSource", schema);
    ASSERT_TRUE(sourceOpt.has_value());
    const auto& logicalSource = sourceOpt.value();
    const auto physical1Opt = sourceCatalog.addPhysicalSource(
        logicalSource,
        NES::INITIAL<NES::WorkerId>,
        "testSource1",
        SourceDescriptor::INVALID_NUMBER_OF_BUFFERS_IN_LOCAL_POOL,
        Configurations::DescriptorConfig::Config{},
        ParserConfig{});
    const auto physical2Opt = sourceCatalog.addPhysicalSource(
        logicalSource,
        NES::INITIAL<NES::WorkerId>,
        "testSource2",
        SourceDescriptor::INVALID_NUMBER_OF_BUFFERS_IN_LOCAL_POOL,
        Configurations::DescriptorConfig::Config{},
        ParserConfig{});

    ASSERT_TRUE(physical1Opt.has_value());
    ASSERT_TRUE(physical2Opt.has_value());
    const auto& physical1 = physical1Opt.value();
    const auto& physical2 = physical2Opt.value();

    auto expectedSources = std::unordered_set{physical1, physical2};

    const auto actualSourcesOpt = sourceCatalog.getPhysicalSources(logicalSource);
    ASSERT_TRUE(actualSourcesOpt.has_value());
    ASSERT_THAT(actualSourcesOpt.value(), testing::ContainerEq(expectedSources));

    ASSERT_TRUE(sourceCatalog.removeLogicalSource(logicalSource));

    ASSERT_FALSE(sourceCatalog.containsLogicalSource(logicalSource));
    ASSERT_FALSE(sourceCatalog.getPhysicalSource(physical1.getPhysicalSourceId()).has_value());
    ASSERT_FALSE(sourceCatalog.getPhysicalSource(physical2.getPhysicalSourceId()).has_value());
}


TEST_F(SourceCatalogTest, ConcurrentSourceCatalogModification)
{
    /// The source catalog might get accessed concurrently, depending on the frontend it is behind.
    /// The current implementation has a simple lock, future implemenations might have some MVCC mechanism.
    /// This test exists mostly to test the source catalog with the thread sanitizer
    constexpr size_t numPhysicalAddThreads = 10;
    constexpr size_t operationsPerThread = 1000;
    constexpr unsigned int concurrentLogicalSourceNames = 3;
    auto sourceCatalog = SourceCatalog{};
    auto schema = Schema{};
    schema.addField("stringField", DataTypeProvider::provideDataType(DataType::Type::VARSIZED));
    schema.addField("intField", DataTypeProvider::provideDataType(DataType::Type::INT32));

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
                    SourceDescriptor::INVALID_NUMBER_OF_BUFFERS_IN_LOCAL_POOL,
                    Configurations::DescriptorConfig::Config{},
                    ParserConfig{});
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
                bool unused = sourceCatalog.removeLogicalSource(*logicalSourceOpt);
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

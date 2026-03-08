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

#include <Operators/StoreLogicalOperator.hpp>

#include <Configurations/Descriptor.hpp>
#include <Replay/BinaryStoreFormat.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>

#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
class StoreLogicalOperatorTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("StoreLogicalOperatorTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup StoreLogicalOperatorTest test class.");
    }
};

TEST_F(StoreLogicalOperatorTest, AcceptsNegativeCompressionLevelForZstd)
{
    auto config = StoreLogicalOperator::validateAndFormatConfig(
        {{"file_path", "/tmp/replay.store"}, {"compression", "Zstd"}, {"compression_level", "-7"}});
    Descriptor descriptor(std::move(config));

    EXPECT_EQ(descriptor.getFromConfig(StoreLogicalOperator::ConfigParameters::COMPRESSION), Replay::BinaryStoreCompressionCodec::Zstd);
    EXPECT_EQ(descriptor.getFromConfig(StoreLogicalOperator::ConfigParameters::COMPRESSION_LEVEL), -7);
}

TEST_F(StoreLogicalOperatorTest, RejectsCompressedStoreWithoutHeader)
{
    ASSERT_EXCEPTION_ERRORCODE(
        StoreLogicalOperator::validateAndFormatConfig(
            {{"file_path", "/tmp/replay.store"}, {"compression", "Zstd"}, {"header", "false"}}),
        ErrorCode::InvalidConfigParameter);
}
}
}

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

#include <Configuration/WorkerConfiguration.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <gtest/gtest.h>

namespace NES
{

/// Size classes are off by default -> no SizeClassConfig is produced.
GTEST_TEST(NodeEngineBuilderTest, SizeClassesDisabledByDefault)
{
    const WorkerConfiguration config;
    EXPECT_FALSE(NodeEngineBuilder::makeSizeClassConfig(config).has_value());
}

/// When enabled, the worker options are translated 1:1 into the SizeClassConfig.
GTEST_TEST(NodeEngineBuilderTest, SizeClassesEnabledTranslatesOptions)
{
    WorkerConfiguration config;
    config.enableBufferSizeClasses.setValue(true);
    config.bufferSizeClassMinBytes.setValue(512);
    config.bufferSizeClassMaxBytes.setValue(65536);
    config.bufferSizeClassProvisioning.setValue(BufferProvisioningPolicy::EagerPerClass);
    config.bufferSizeClassBudgetBytes.setValue(0);
    config.bufferSizeClassBuffersPerClass.setValue(128);

    const auto result = NodeEngineBuilder::makeSizeClassConfig(config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->minClassSize, 512U);
    EXPECT_EQ(result->maxClassSize, 65536U);
    EXPECT_EQ(result->policy, BufferProvisioningPolicy::EagerPerClass);
    EXPECT_EQ(result->buffersPerClass, 128U);
}

/// A min class size larger than the max is rejected with a clear configuration error.
GTEST_TEST(NodeEngineBuilderTest, MinGreaterThanMaxThrows)
{
    WorkerConfiguration config;
    config.enableBufferSizeClasses.setValue(true);
    config.bufferSizeClassMinBytes.setValue(65536);
    config.bufferSizeClassMaxBytes.setValue(512);

    EXPECT_ANY_THROW([[maybe_unused]] auto result = NodeEngineBuilder::makeSizeClassConfig(config));
}

}

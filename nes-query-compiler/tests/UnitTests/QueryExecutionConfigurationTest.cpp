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

#include <cstdint>
#include <gtest/gtest.h>
#include <yaml-cpp/yaml.h>
#include <QueryExecutionConfiguration.hpp>

/// NOLINTBEGIN(readability-magic-numbers) -- configuration tests compare against literal byte counts throughout

namespace NES
{

TEST(QueryExecutionConfigurationTest, DefaultByteSizesAreUnchanged)
{
    const QueryExecutionConfiguration config;
    EXPECT_EQ(config.pageSize.getValue(), 1024);
    EXPECT_EQ(config.operatorBufferSize.getValue(), 4096);
}

TEST(QueryExecutionConfigurationTest, ByteSizesAcceptHumanReadableAmounts)
{
    QueryExecutionConfiguration config;
    YAML::Node node;
    node["page_size"] = "4KiB";
    node["operator_buffer_size"] = "1Mi";
    config.overwriteConfigWithYAMLNode(node);
    EXPECT_EQ(config.pageSize.getValue(), 4096);
    EXPECT_EQ(config.operatorBufferSize.getValue(), 1ULL << 20);
}

TEST(QueryExecutionConfigurationTest, ByteSizesAcceptPlainByteCounts)
{
    QueryExecutionConfiguration config;
    YAML::Node node;
    node["page_size"] = "2048";
    node["operator_buffer_size"] = "8192";
    config.overwriteConfigWithYAMLNode(node);
    EXPECT_EQ(config.pageSize.getValue(), 2048);
    EXPECT_EQ(config.operatorBufferSize.getValue(), 8192);
}

}

/// NOLINTEND(readability-magic-numbers)

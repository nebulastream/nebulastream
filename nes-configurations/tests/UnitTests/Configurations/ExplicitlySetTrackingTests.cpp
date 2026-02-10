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
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/BaseOption.hpp>
#include <Configurations/ConfigValuePrinter.hpp>
#include <Configurations/Enums/EnumOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/SequenceOption.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>

namespace NES
{

enum class TestEnum
{
    A,
    B,
    C
};

struct InnerConfig : BaseConfiguration
{
    InnerConfig(std::string name, std::string description) : BaseConfiguration(std::move(name), std::move(description)) { }
    InnerConfig() = default;

    ScalarOption<size_t> threads{"threads", "4", "Number of threads"};
    ScalarOption<std::string> name{"name", "default", "Name"};

protected:
    std::vector<BaseOption*> getOptions() override { return {&threads, &name}; }
};

struct TestConfig : BaseConfiguration
{
    ScalarOption<std::string> host{"host", "localhost", "Host"};
    ScalarOption<size_t> port{"port", "8080", "Port"};
    EnumOption<TestEnum> mode{"mode", TestEnum::A, "Mode"};
    InnerConfig inner{"inner", "Inner configuration"};

protected:
    std::vector<BaseOption*> getOptions() override { return {&host, &port, &mode, &inner}; }
};

class ExplicitlySetTrackingTest : public Testing::BaseIntegrationTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("ExplicitlySetTrackingTest.log", LogLevel::LOG_DEBUG);
    }
};

TEST_F(ExplicitlySetTrackingTest, DefaultValuesAreNotExplicitlySet)
{
    TestConfig config;
    EXPECT_FALSE(config.host.isExplicitlySet());
    EXPECT_FALSE(config.port.isExplicitlySet());
    EXPECT_FALSE(config.mode.isExplicitlySet());
    EXPECT_FALSE(config.inner.threads.isExplicitlySet());
    EXPECT_FALSE(config.isExplicitlySet());
}

TEST_F(ExplicitlySetTrackingTest, SetValueMarksAsExplicitlySet)
{
    TestConfig config;
    config.host.setValue("example.com");
    EXPECT_TRUE(config.host.isExplicitlySet());
    EXPECT_FALSE(config.port.isExplicitlySet());
    EXPECT_TRUE(config.isExplicitlySet());
}

TEST_F(ExplicitlySetTrackingTest, IsExplicitlySetIsTransitiveThroughNestedConfigs)
{
    TestConfig config;
    EXPECT_FALSE(config.inner.isExplicitlySet());
    EXPECT_FALSE(config.isExplicitlySet());

    config.inner.threads.setValue(16);

    EXPECT_TRUE(config.inner.threads.isExplicitlySet());
    EXPECT_TRUE(config.inner.isExplicitlySet());
    EXPECT_TRUE(config.isExplicitlySet());
    // Sibling options remain unset
    EXPECT_FALSE(config.inner.name.isExplicitlySet());
    EXPECT_FALSE(config.host.isExplicitlySet());
}

TEST_F(ExplicitlySetTrackingTest, OperatorAssignmentMarksAsExplicitlySet)
{
    TestConfig config;
    config.port = static_cast<size_t>(9090);
    EXPECT_TRUE(config.port.isExplicitlySet());
    EXPECT_FALSE(config.host.isExplicitlySet());
}

TEST_F(ExplicitlySetTrackingTest, EnumAssignmentMarksAsExplicitlySet)
{
    TestConfig config;
    config.mode = TestEnum::B;
    EXPECT_TRUE(config.mode.isExplicitlySet());
}

TEST_F(ExplicitlySetTrackingTest, ClearResetsExplicitlySet)
{
    TestConfig config;
    config.host.setValue("example.com");
    EXPECT_TRUE(config.host.isExplicitlySet());
    config.host.clear();
    EXPECT_FALSE(config.host.isExplicitlySet());
    EXPECT_EQ(config.host.getValue(), "localhost");
}

TEST_F(ExplicitlySetTrackingTest, ParseFromYAMLMarksAsExplicitlySet)
{
    TestConfig config;
    YAML::Node node;
    node["host"] = "yaml-host";
    node["inner"]["threads"] = "12";
    config.overwriteConfigWithYAMLNode(node);

    EXPECT_TRUE(config.host.isExplicitlySet());
    EXPECT_TRUE(config.inner.threads.isExplicitlySet());
    EXPECT_FALSE(config.port.isExplicitlySet());
    EXPECT_FALSE(config.inner.name.isExplicitlySet());
}

TEST_F(ExplicitlySetTrackingTest, ApplyExplicitlySetFromOnlyOverridesSetValues)
{
    TestConfig base;
    base.host.setValue("base-host");
    base.inner.threads.setValue(8);

    TestConfig overlay;
    overlay.port.setValue(9090);

    base.applyExplicitlySetFrom(overlay);

    // base-host should be preserved (overlay.host was not explicitly set)
    EXPECT_EQ(base.host.getValue(), "base-host");
    // port should be overridden (overlay.port was explicitly set)
    EXPECT_EQ(base.port.getValue(), static_cast<size_t>(9090));
    // inner.threads should be preserved (overlay.inner.threads was not explicitly set)
    EXPECT_EQ(base.inner.threads.getValue(), static_cast<size_t>(8));
}

TEST_F(ExplicitlySetTrackingTest, ApplyExplicitlySetFromWithNestedConfig)
{
    TestConfig base;
    base.inner.threads.setValue(8);
    base.inner.name.setValue("base-name");

    TestConfig overlay;
    overlay.inner.threads.setValue(16);

    base.applyExplicitlySetFrom(overlay);

    // threads should be overridden
    EXPECT_EQ(base.inner.threads.getValue(), static_cast<size_t>(16));
    // name should be preserved
    EXPECT_EQ(base.inner.name.getValue(), "base-name");
}

TEST_F(ExplicitlySetTrackingTest, ApplyExplicitlySetFromDoesNothingWhenNothingSet)
{
    TestConfig base;
    base.host.setValue("base-host");
    base.port.setValue(9090);

    TestConfig overlay; // nothing explicitly set

    base.applyExplicitlySetFrom(overlay);

    EXPECT_EQ(base.host.getValue(), "base-host");
    EXPECT_EQ(base.port.getValue(), static_cast<size_t>(9090));
}

TEST_F(ExplicitlySetTrackingTest, CopyPreservesExplicitlySetFlag)
{
    TestConfig original;
    original.host.setValue("set-host");
    EXPECT_TRUE(original.host.isExplicitlySet());

    TestConfig copy = original;
    EXPECT_TRUE(copy.host.isExplicitlySet());
    EXPECT_FALSE(copy.port.isExplicitlySet());
}

TEST_F(ExplicitlySetTrackingTest, ApplyExplicitlySetFromYAMLParsedConfig)
{
    // Simulates the systest use case: topology YAML sets threads, CLI has defaults
    TestConfig topologyConfig;
    YAML::Node yamlNode;
    yamlNode["inner"]["threads"] = "12";
    topologyConfig.overwriteConfigWithYAMLNode(yamlNode);

    TestConfig cliConfig; // all defaults, nothing explicitly set

    // Start with topology, overlay CLI
    topologyConfig.applyExplicitlySetFrom(cliConfig);

    // Topology value should be preserved
    EXPECT_EQ(topologyConfig.inner.threads.getValue(), static_cast<size_t>(12));
    // Default values remain
    EXPECT_EQ(topologyConfig.host.getValue(), "localhost");
}

TEST_F(ExplicitlySetTrackingTest, ConfigValuePrinterShowsPathsAndDefaults)
{
    TestConfig config;
    config.inner.threads.setValue(12);

    std::stringstream ss;
    ConfigValuePrinter printer(ss);
    config.accept(printer);

    const auto output = ss.str();
    EXPECT_NE(output.find("host: localhost (default)"), std::string::npos);
    EXPECT_NE(output.find("port: 8080 (default)"), std::string::npos);
    EXPECT_NE(output.find("mode: A (default)"), std::string::npos);
    EXPECT_NE(output.find("inner.threads: 12\n"), std::string::npos);
    EXPECT_NE(output.find("inner.name: default (default)"), std::string::npos);
    // Explicitly set value should NOT have "(default)" suffix
    EXPECT_EQ(output.find("inner.threads: 12 (default)"), std::string::npos);
}

TEST_F(ExplicitlySetTrackingTest, ConfigValuePrinterWithYAMLOverrides)
{
    TestConfig config;
    YAML::Node node;
    node["host"] = "my-host";
    node["inner"]["threads"] = "16";
    config.overwriteConfigWithYAMLNode(node);

    std::stringstream ss;
    ConfigValuePrinter printer(ss);
    config.accept(printer);

    const auto output = ss.str();
    EXPECT_NE(output.find("host: my-host\n"), std::string::npos);
    EXPECT_NE(output.find("inner.threads: 16\n"), std::string::npos);
    EXPECT_NE(output.find("port: 8080 (default)"), std::string::npos);
}

}

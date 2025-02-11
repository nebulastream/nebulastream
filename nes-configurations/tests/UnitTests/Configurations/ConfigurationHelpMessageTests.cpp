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
#include <cstdint>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/BaseOption.hpp>
#include <Configurations/Enums/EnumOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Util.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <sys/types.h>
#include <BaseIntegrationTest.hpp>

namespace NES
{

using namespace Configurations;

class ConfigHelpMessageTest : public Testing::BaseIntegrationTest
{
public:
    static void SetUpTestSuite()
    {
        NES::Logger::setupLogging("ConfigHelpMessageTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup Configuration Help Message test class.");
    }
};

TEST_F(ConfigHelpMessageTest, GenerateHelpMessageForDifferentTypes)
{
    enum class TestEnum : uint8_t
    {
        YES,
        NO
    };

    struct InnerConfiguration : BaseConfiguration
    {
    protected:
        std::vector<BaseOption*> getOptions() override { return {&b}; }

    public:
        InnerConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { }
        InnerConfiguration() = default;
        ScalarOption<size_t> b{"B", "54", "This is Inner Option B"};
    };

    struct TestConfig : BaseConfiguration
    {
        ScalarOption<std::string> a{"A", "False", "This is Option A"};
        ScalarOption<size_t> b{"B", "42", "This is Option B"};
        EnumOption<TestEnum> c{"C", TestEnum::YES, "This is Option C"};
        ScalarOption<ssize_t> d{"D", "-42", "This is Option D"};
        SequenceOption<InnerConfiguration> e{"E", "This is Option E"};
        InnerConfiguration f{"F", "This is Option F"};
        ScalarOption<float> g{"G", "1.25", "This is Option G"};
        TestConfig() : BaseConfiguration("TestConfig", "A Description") {}

    protected:
        std::vector<BaseOption*> getOptions() override { return {&a, &b, &c, &d, &e, &f, &g}; }
    };

    TestConfig config;

    config.a.setValue("123123");

    std::stringstream helpMessage;
    Configurations::print(config, helpMessage);

    EXPECT_EQ(
        helpMessage.str(),
        R"(TestConfig: A Description
    A: 123123
    B: This is Option B (42, Unsigned Integer)
    C: This is Option C (YES, Enum)
    D: This is Option D (-42, Signed Integer)
    E: This is Option E (Multiple)
    F: This is Option F
        B: This is Inner Option B (54, Unsigned Integer)
    G: This is Option G (1.25, Float))");
}

TEST_F(ConfigHelpMessageTest, GenerateHelpMessageSequence)
{
    struct Config : BaseConfiguration
    {
        Config(const std::string& name, const std::string& description) : BaseConfiguration(name, description)
        {
            sequenceOfStrings.add(StringOption{"first", "Are", "first string"});
            sequenceOfStrings.add(StringOption{"second", "we", "second string"});
            sequenceOfStrings.add(StringOption{"third", "testing", "third string"});

            sequenceOfFloats.add(FloatOption{"first", "2.1", "first float"});
            sequenceOfFloats.add(FloatOption{"second", "2.2", "second float"});
            sequenceOfFloats.add(FloatOption{"third", "2.3", "third float"});

            sequenceOfBooleans.add(BoolOption{"first", "true", "first bool"});
            sequenceOfBooleans.add(BoolOption{"second", "false", "second bool"});
            sequenceOfBooleans.add(BoolOption{"third", "true", "third bool"});

            sequenceOfIntegers.add(UIntOption{"first", "1", "first int"});
            sequenceOfIntegers.add(UIntOption{"second", "2", "second int"});
            sequenceOfIntegers.add(UIntOption{"third", "3", "third int"});
        }
        Config() : Config("Config", "DefaultDescription") { }

        std::vector<BaseOption*> getOptions() override
        {
            return {&sequenceOfStrings, &sequenceOfFloats, &sequenceOfBooleans, &sequenceOfIntegers};
        }

        SequenceOption<ScalarOption<std::string>> sequenceOfStrings = {"Strings", "A sequence of strings."};
        SequenceOption<ScalarOption<float>> sequenceOfFloats = {"Floats", "A sequence of floats."};
        SequenceOption<ScalarOption<bool>> sequenceOfBooleans = {"Booleans", "A sequence of booleans."};
        SequenceOption<ScalarOption<uint64_t>> sequenceOfIntegers = {"Integers", "A sequence of integers."};
    };
    std::stringstream helpMessage;
    Configurations::generateHelp<Config>(helpMessage);
    EXPECT_EQ(
        helpMessage.str(),
        R"(Config: DefaultDescription
    Strings: A sequence of strings. (Multiple)
    Floats: A sequence of floats. (Multiple)
    Booleans: A sequence of booleans. (Multiple)
    Integers: A sequence of integers. (Multiple)
)");
}

}

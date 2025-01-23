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
#include <cstdio>
#include <iostream>
#include <vector>
#include <Configuration/WorkerConfiguration.hpp>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/BaseOption.hpp>
#include <Configurations/Enums/CompilationStrategy.hpp>
#include <Configurations/Enums/NautilusBackend.hpp>
#include <Configurations/ProtobufConfigCache.hpp>
#include <Configurations/ProtobufDeserializeVisitor.hpp>
#include <Configurations/ProtobufMessageTypeBuilderOptionVisitor.hpp>
#include <Configurations/ProtobufSerializeOptionVisitor.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/SequenceOption.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <google/protobuf/descriptor.pb.h>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
using namespace Configurations;

class ConfigSerializationTest : public Testing::BaseIntegrationTest
{
public:
    static void SetUpTestSuite()
    {
        NES::Logger::setupLogging("ConfigSerialization.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ConfigSerialization test class.");
    }
};

TEST_F(ConfigSerializationTest, EmptyMessageTest)
{
    ProtobufConfigCache cache;
    const WorkerConfiguration configuration("root", "");
    cache.printFile();
}

TEST_F(ConfigSerializationTest, InvalidConfigParameterTest)
{
    class TestConfig : public BaseConfiguration
    {
    public:
        TestConfig(const std::string& name, const std::string& description) : BaseConfiguration(name, description) {};

        ScalarOption<ssize_t> a{"A", "-42", "This is Option A"};

    protected:
        std::vector<BaseOption*> getOptions() override { return {&a}; }
    };

    ProtobufConfigCache cache;
    const TestConfig testConfiguration("test configuration", "");
    ScalarOption<std::string> option("NonExistentField", "default", "description");

    auto* message = cache.getEmptyMessage<TestConfig>();

    ProtobufSerializeOptionVisitor serializer(message);
    EXPECT_THROW(serializer.push(option), NES::Exception);
    ProtobufDeserializeVisitor deserializer(message);
    EXPECT_THROW(deserializer.push(option), NES::Exception);
}

TEST_F(ConfigSerializationTest, GetValuesTest)
{
    SequenceOption<UIntOption> sequence("testSequence", "a test sequence");
    const UIntOption option1("option1", "1", "first option");

    sequence.add(option1);

    std::vector<UIntOption> values = sequence.getValues();
    ASSERT_EQ(values.size(), 1);
    EXPECT_EQ(values[0].getValue(), 1);
}

TEST_F(ConfigSerializationTest, ClearSequenceTest)
{
    SequenceOption<UIntOption> sequence("TestSequence", "A test sequence");
    const UIntOption option1("Option1", "1", "First option");

    sequence.add(option1);

    EXPECT_FALSE(sequence.empty());
    sequence.clear();
    EXPECT_TRUE(sequence.empty());
}


TEST_F(ConfigSerializationTest, ClearBaseConfigTest)
{
    struct TestConfig : BaseConfiguration
    {
        TestConfig(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { }
        TestConfig() = default;

        std::vector<BaseOption*> getOptions() override { return {&string}; }

        ScalarOption<std::string> string = {"String", "foo", "A string"};
    };
    TestConfig config("TestBaseConfig", "A test configuration");
    config.string.setValue("bruh");
    EXPECT_NE(config.string.getValue(), config.string.getDefaultValue());
    config.clear();
    EXPECT_EQ(config.string.getValue(), config.string.getDefaultValue());
}

TEST_F(ConfigSerializationTest, ScalarEqualityOperator)
{
    UIntOption option1("option1", "1", "first option");
    const UIntOption option2("option2", "2", "second option");

    EXPECT_TRUE(option1.operator==(option1));

    EXPECT_FALSE(option1.operator==(option2));
}

TEST_F(ConfigSerializationTest, PrintFileTest)
{
    google::protobuf::FileDescriptorProto proto;
    ProtobufMessageTypeBuilderOptionVisitor visitor{proto, "TestVisitor"};
    visitor.printFile();
}

TEST_F(ConfigSerializationTest, SequenceAddTest)
{
    struct TestConfig : BaseConfiguration
    {
        TestConfig(const std::string& name, const std::string& description) : BaseConfiguration(name, description)
        {
            sequenceOfIntegers.add(UIntOption{"first", "6", "first int"});
        }
        TestConfig() = default;

        std::vector<BaseOption*> getOptions() override { return {&sequenceOfIntegers}; }

        SequenceOption<ScalarOption<uint64_t>> sequenceOfIntegers = {"Integers", "A sequence of integers."};
    };

    TestConfig config{"test configuration", "A test configuration"};
    constexpr uint64_t integerValue = 7;

    EXPECT_EQ(config.sequenceOfIntegers.size(), 1);
    config.sequenceOfIntegers.add(integerValue);
    EXPECT_EQ(config.sequenceOfIntegers.size(), 2);
}

TEST_F(ConfigSerializationTest, EmptySequenceTest)
{
    const SequenceOption<UIntOption> sequence("testSequence", "a test sequence");
    EXPECT_TRUE(sequence.empty());
}

TEST_F(ConfigSerializationTest, SequenceToStringTest)
{
    SequenceOption<ScalarOption<int>> sequence("testSequence", "a test sequence");
    const std::string expectedString = "Name: testSequence\nDescription: a test sequence\n";
    EXPECT_EQ(sequence.toString(), expectedString);
}

TEST_F(ConfigSerializationTest, SequenceSerializationTest)
{
    struct SourceConfig : BaseConfiguration
    {
        SourceConfig(const std::string& name, const std::string& description) : BaseConfiguration(name, description)
        {
            sequenceOfStrings.add(StringOption("first", "We", "first string"));
            sequenceOfStrings.add(StringOption{"second", "are", "second string"});
            sequenceOfStrings.add(StringOption{"third", "correctly", "third string"});

            sequenceOfFloats.add(FloatOption{"first", "1.1", "first float"});
            sequenceOfFloats.add(FloatOption{"second", "1.2", "second float"});
            sequenceOfFloats.add(FloatOption{"third", "1.3", "third float"});

            sequenceOfBooleans.add(BoolOption{"first", "false", "first bool"});
            sequenceOfBooleans.add(BoolOption{"second", "true", "second bool"});
            sequenceOfBooleans.add(BoolOption{"third", "false", "third bool"});

            sequenceOfIntegers.add(UIntOption{"first", "6", "first int"});
            sequenceOfIntegers.add(UIntOption{"second", "7", "second int"});
            sequenceOfIntegers.add(UIntOption{"third", "8", "third int"});
        }

        std::vector<BaseOption*> getOptions() override
        {
            return {&sequenceOfStrings, &sequenceOfFloats, &sequenceOfBooleans, &sequenceOfIntegers};
        }

        SequenceOption<ScalarOption<std::string>> sequenceOfStrings{"Strings", "A sequence of strings."};
        SequenceOption<ScalarOption<float>> sequenceOfFloats = {"Floats", "A sequence of floats."};
        SequenceOption<ScalarOption<bool>> sequenceOfBooleans = {"Booleans", "A sequence of booleans."};
        SequenceOption<ScalarOption<uint64_t>> sequenceOfIntegers = {"Integers", "A sequence of integers."};
    };

    struct TargetConfig : BaseConfiguration
    {
        TargetConfig(const std::string& name, const std::string& description) : BaseConfiguration(name, description)
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
        std::vector<BaseOption*> getOptions() override
        {
            return {&sequenceOfStrings, &sequenceOfFloats, &sequenceOfBooleans, &sequenceOfIntegers};
        }

        SequenceOption<ScalarOption<std::string>> sequenceOfStrings = {"Strings", "A sequence of strings."};
        SequenceOption<ScalarOption<float>> sequenceOfFloats = {"Floats", "A sequence of floats."};
        SequenceOption<ScalarOption<bool>> sequenceOfBooleans = {"Booleans", "A sequence of booleans."};
        SequenceOption<ScalarOption<uint64_t>> sequenceOfIntegers = {"Integers", "A sequence of integers."};
    };

    ProtobufConfigCache cache;

    /// Source configuration (before serialization)
    SourceConfig sourceConfig{"source configuration", "Values to be serialized."};

    /// Serialize the source configuration
    auto* message = cache.getEmptyMessage<SourceConfig>();
    ProtobufSerializeOptionVisitor serializer(message);
    sourceConfig.accept(serializer);

    /// Deserialize message: overwrite target configuration with values from serialized source configuration
    TargetConfig targetConfig{"target configuration", "Values to be overwritten."};
    ProtobufDeserializeVisitor deserializer(message);
    for (size_t i = 0; i < sourceConfig.getOptions().size(); ++i)
    {
        if (auto* sourceStringSequence = dynamic_cast<SequenceOption<ScalarOption<std::string>>*>(sourceConfig.getOptions()[i]))
        {
            auto* targetStringSequence = dynamic_cast<SequenceOption<ScalarOption<std::string>>*>(targetConfig.getOptions()[i]);
            EXPECT_EQ(sourceStringSequence->size(), targetStringSequence->size());
            for (size_t j = 0; j < sourceStringSequence->size(); ++j)
            {
                EXPECT_NE(sourceStringSequence->operator[](j).getValue(), targetStringSequence->operator[](j).getValue());
            }
        }
        else if (auto* sourceFloatSequence = dynamic_cast<SequenceOption<ScalarOption<float>>*>(sourceConfig.getOptions()[i]))
        {
            auto* targetFloatSequence = dynamic_cast<SequenceOption<ScalarOption<float>>*>(targetConfig.getOptions()[i]);
            EXPECT_EQ(sourceFloatSequence->size(), targetFloatSequence->size());
            for (size_t j = 0; j < sourceFloatSequence->size(); ++j)
            {
                EXPECT_NE(sourceFloatSequence->operator[](j).getValue(), targetFloatSequence->operator[](j).getValue());
            }
        }
        else if (auto* sourceBoolSequence = dynamic_cast<SequenceOption<ScalarOption<bool>>*>(sourceConfig.getOptions()[i]))
        {
            auto* targetBoolSequence = dynamic_cast<SequenceOption<ScalarOption<bool>>*>(targetConfig.getOptions()[i]);
            EXPECT_EQ(sourceBoolSequence->size(), targetBoolSequence->size());
            for (size_t j = 0; j < sourceBoolSequence->size(); ++j)
            {
                EXPECT_NE(sourceBoolSequence->operator[](j).getValue(), targetBoolSequence->operator[](j).getValue());
            }
        }
        else if (auto* sourceIntSequence = dynamic_cast<SequenceOption<ScalarOption<uint64_t>>*>(sourceConfig.getOptions()[i]))
        {
            auto* targetIntSequence = dynamic_cast<SequenceOption<ScalarOption<uint64_t>>*>(targetConfig.getOptions()[i]);
            EXPECT_EQ(sourceIntSequence->size(), targetIntSequence->size());
            for (size_t j = 0; j < sourceIntSequence->size(); ++j)
            {
                EXPECT_NE(sourceIntSequence->operator[](j).getValue(), targetIntSequence->operator[](j).getValue());
            }
        }
    }
    targetConfig.accept(deserializer);
    /// After overwriting
    for (size_t i = 0; i < sourceConfig.getOptions().size(); ++i)
    {
        if (auto* sourceStringSequence = dynamic_cast<SequenceOption<ScalarOption<std::string>>*>(sourceConfig.getOptions()[i]))
        {
            auto* targetStringSequence = dynamic_cast<SequenceOption<ScalarOption<std::string>>*>(targetConfig.getOptions()[i]);
            EXPECT_EQ(sourceStringSequence->size(), targetStringSequence->size());
            for (size_t j = 0; j < sourceStringSequence->size(); ++j)
            {
                EXPECT_EQ(sourceStringSequence->operator[](j).getValue(), targetStringSequence->operator[](j).getValue());
            }
        }
        else if (auto* sourceFloatSequence = dynamic_cast<SequenceOption<ScalarOption<float>>*>(sourceConfig.getOptions()[i]))
        {
            auto* targetFloatSequence = dynamic_cast<SequenceOption<ScalarOption<float>>*>(targetConfig.getOptions()[i]);
            EXPECT_EQ(sourceFloatSequence->size(), targetFloatSequence->size());
            for (size_t j = 0; j < sourceFloatSequence->size(); ++j)
            {
                EXPECT_EQ(sourceFloatSequence->operator[](j).getValue(), targetFloatSequence->operator[](j).getValue());
            }
        }
        else if (auto* sourceBoolSequence = dynamic_cast<SequenceOption<ScalarOption<bool>>*>(sourceConfig.getOptions()[i]))
        {
            auto* targetBoolSequence = dynamic_cast<SequenceOption<ScalarOption<bool>>*>(targetConfig.getOptions()[i]);
            EXPECT_EQ(sourceBoolSequence->size(), targetBoolSequence->size());
            for (size_t j = 0; j < sourceBoolSequence->size(); ++j)
            {
                EXPECT_EQ(sourceBoolSequence->operator[](j).getValue(), targetBoolSequence->operator[](j).getValue());
            }
        }
        else if (auto* sourceIntSequence = dynamic_cast<SequenceOption<ScalarOption<uint64_t>>*>(sourceConfig.getOptions()[i]))
        {
            auto* targetIntSequence = dynamic_cast<SequenceOption<ScalarOption<uint64_t>>*>(targetConfig.getOptions()[i]);
            EXPECT_EQ(sourceIntSequence->size(), targetIntSequence->size());
            for (size_t j = 0; j < sourceIntSequence->size(); ++j)
            {
                EXPECT_EQ(sourceIntSequence->operator[](j).getValue(), targetIntSequence->operator[](j).getValue());
            }
        }
    }
}

TEST_F(ConfigSerializationTest, SerializationTest)
{
    ProtobufConfigCache cache;

    /// Test values
    constexpr uint32_t rpcValue = 10;
    constexpr uint32_t numberOfBuffersInGlobalBufferManagerValue = 98765;
    constexpr uint32_t numberOfBuffersInSourceLocalBufferPoolValue = 128;
    constexpr uint32_t bufferSizeInBytesValue = 42;
    constexpr uint32_t numberOfPartitionsValue = 3;
    constexpr uint32_t pageSizeValue = 50;
    constexpr uint32_t preAllocPageCntValue = 4;
    constexpr uint32_t maxHasTableSizeValue = 7;

    /// Source configuration (before serialization)
    WorkerConfiguration sourceConfiguration("source configuration", "Values to be serialized.");

    /// Modify source configuration so it does not contain the default values
    sourceConfiguration.localWorkerHost.setValue("0.0.0.0");
    sourceConfiguration.rpcPort.setValue(rpcValue);
    sourceConfiguration.numberOfBuffersInGlobalBufferManager.setValue(numberOfBuffersInGlobalBufferManagerValue);
    sourceConfiguration.numberOfBuffersInSourceLocalBufferPool.setValue(numberOfBuffersInSourceLocalBufferPoolValue);
    sourceConfiguration.bufferSizeInBytes.setValue(bufferSizeInBytesValue);
    sourceConfiguration.logLevel.setValue(LogLevel::LOG_NONE);
    sourceConfiguration.configPath.setValue("THE PATH");

    sourceConfiguration.queryCompiler.compilationStrategy.setValue(QueryCompilation::CompilationStrategy::FAST);
    sourceConfiguration.queryCompiler.nautilusBackend.setValue(QueryCompilation::NautilusBackend::INTERPRETER);
    sourceConfiguration.queryCompiler.useCompilationCache.setValue(true);
    sourceConfiguration.queryCompiler.numberOfPartitions.setValue(numberOfPartitionsValue);
    sourceConfiguration.queryCompiler.pageSize.setValue(pageSizeValue);
    sourceConfiguration.queryCompiler.preAllocPageCnt.setValue(preAllocPageCntValue);
    sourceConfiguration.queryCompiler.maxHashTableSize.setValue(maxHasTableSizeValue);

    /// Serialize source worker configuration
    auto* message = cache.getEmptyMessage<WorkerConfiguration>();
    cache.printFile();
    ProtobufSerializeOptionVisitor serializer(message);
    sourceConfiguration.accept(serializer);

    /// Target worker configuration with default values
    WorkerConfiguration targetConfiguration("target configuration", "Values to be overwritten.");

    /// Deserialize message: overwrite target configuration with values from serialized source configuration
    ProtobufDeserializeVisitor deserializer(message);
    EXPECT_NE(sourceConfiguration.toString(), targetConfiguration.toString());
    targetConfiguration.accept(deserializer);
    EXPECT_EQ(sourceConfiguration.toString(), targetConfiguration.toString());
}

TEST_F(ConfigSerializationTest, ScalarSerializationTest)
{
    class TestConfig : public BaseConfiguration
    {
    public:
        TestConfig(const std::string& name, const std::string& description) : BaseConfiguration(name, description) {};

        ScalarOption<ssize_t> a{"A", "-42", "This is Option A"};
        ScalarOption<float> b{"B", "1.25", "This is Option B"};

    protected:
        std::vector<BaseOption*> getOptions() override { return {&a, &b}; }
    };

    ProtobufConfigCache cache;

    /// Source configuration
    TestConfig sourceConfig{"source configuration", "Values to be serialized."};
    std::cout << sourceConfig.a.getName() << '\n';

    /// Modify configuration so it does not contain the default values
    constexpr int32_t valueA = -52;
    constexpr float valueB = 2.35;

    sourceConfig.a.setValue(valueA);
    sourceConfig.b.setValue(valueB);

    /// Serialize configuration
    auto* message = cache.getEmptyMessage<TestConfig>();
    ProtobufSerializeOptionVisitor serializer(message);
    sourceConfig.accept(serializer);

    /// Target configuration with default values
    TestConfig targetConfig{"target configuration", "Values to be overwritten."};

    /// Deserialize message: overwrite target configuration with values from serialized source configuration
    ProtobufDeserializeVisitor deserializer(message);
    EXPECT_NE(sourceConfig.toString(), targetConfig.toString());
    targetConfig.accept(deserializer);
    EXPECT_EQ(sourceConfig.toString(), targetConfig.toString());
}

}

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
#include <Configurations/PrintingVisitor.hpp>
#include <Configurations/ProtobufConfigCache.hpp>
#include <Configurations/ProtobufDeserializeVisitor.hpp>
#include <Configurations/ProtobufSerializeOptionVisitor.hpp>
#include <Util/Logger/Logger.hpp>
#include <google/protobuf/extension_set.h>
#include <nautilus/std/iostream.h>

#include <BaseIntegrationTest.hpp>


namespace NES
{
using namespace Configurations;

class ConfigSerializationTest : public Testing::BaseIntegrationTest
{
public:
    static void SetUpTestCase()
    {
        NES::Logger::setupLogging("Config.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ConfigSerialization test class.");
    }
};

TEST_F(ConfigSerializationTest, EmptyMessageTest)
{
    ProtobufConfigCache cache;
    WorkerConfiguration configuration("root", "");
    auto* message = cache.getEmptyMessage<WorkerConfiguration>();
    cache.printFile();
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
            sequenceOfStrings.add(StringOption{"fourth", "testing", "fourth string"});
            sequenceOfStrings.add(StringOption{"fifth", "!", "fifth string"});

            sequenceOfFloats.add(FloatOption{"first", "1.1", "first float"});
            sequenceOfFloats.add(FloatOption{"second", "1.2", "second float"});
            sequenceOfFloats.add(FloatOption{"third", "1.3", "third float"});
            sequenceOfFloats.add(FloatOption{"fourth", "1.4", "fourth float"});
            sequenceOfFloats.add(FloatOption{"fifth", "1.5", "fifth float"});

            sequenceOfBooleans.add(BoolOption{"first", "false", "first bool"});
            sequenceOfBooleans.add(BoolOption{"second", "true", "second bool"});
            sequenceOfBooleans.add(BoolOption{"third", "false", "third bool"});
            sequenceOfBooleans.add(BoolOption{"fourth", "true", "fourth bool"});
            sequenceOfBooleans.add(BoolOption{"fifth", "false", "fifth bool"});

            sequenceOfIntegers.add(UIntOption{"first", "6", "first int"});
            sequenceOfIntegers.add(UIntOption{"second", "7", "second int"});
            sequenceOfIntegers.add(UIntOption{"third", "8", "third int"});
            sequenceOfIntegers.add(UIntOption{"fourth", "9", "fourth int"});
            sequenceOfIntegers.add(UIntOption{"fifth", "10", "fifth int"});
        }
        SourceConfig() = default;

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
            sequenceOfStrings.add(StringOption{"fourth", "correctly", "fourth string"});
            sequenceOfStrings.add(StringOption{"fifth", "?", "fifth string"});

            sequenceOfFloats.add(FloatOption{"first", "2.1", "first float"});
            sequenceOfFloats.add(FloatOption{"second", "2.2", "second float"});
            sequenceOfFloats.add(FloatOption{"third", "2.3", "third float"});
            sequenceOfFloats.add(FloatOption{"fourth", "2.4", "fourth float"});
            sequenceOfFloats.add(FloatOption{"fifth", "2.5", "fifth float"});

            sequenceOfBooleans.add(BoolOption{"first", "true", "first bool"});
            sequenceOfBooleans.add(BoolOption{"second", "false", "second bool"});
            sequenceOfBooleans.add(BoolOption{"third", "true", "third bool"});
            sequenceOfBooleans.add(BoolOption{"fourth", "false", "fourth bool"});
            sequenceOfBooleans.add(BoolOption{"fifth", "true", "fifth bool"});

            sequenceOfIntegers.add(UIntOption{"first", "1", "first int"});
            sequenceOfIntegers.add(UIntOption{"second", "2", "second int"});
            sequenceOfIntegers.add(UIntOption{"third", "3", "third int"});
            sequenceOfIntegers.add(UIntOption{"fourth", "4", "fourth int"});
            sequenceOfIntegers.add(UIntOption{"fifth", "5", "fifth int"});
        }
        TargetConfig() = default;

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
    std::cout << "----- Before overwriting -----" << '\n' << std::endl;
    for (size_t i = 0; i < sourceConfig.getOptions().size(); ++i)
    {
        if (auto* sourceOption = dynamic_cast<SequenceOption<ScalarOption<std::string>>*>(sourceConfig.getOptions()[i]))
        {
            std::cout << "String Sequences" << std::endl;
            auto* targetOption = dynamic_cast<SequenceOption<ScalarOption<std::string>>*>(targetConfig.getOptions()[i]);
            EXPECT_EQ(sourceOption->size(), targetOption->size());
            for (size_t j = 0; j < sourceOption->size(); ++j)
            {
                std::cout << "Target String: '" << targetOption->operator[](j).getValue() << "' "
                          << "Source String: '" << sourceOption->operator[](j).getValue() << "' "<< std::endl;
                EXPECT_NE(sourceOption->operator[](j).getValue(), targetOption->operator[](j).getValue());
            }
        }
        else if (auto* sourceOption = dynamic_cast<SequenceOption<ScalarOption<float>>*>(sourceConfig.getOptions()[i]))
        {
            std::cout << "Float Sequences" << std::endl;
            auto* targetOption = dynamic_cast<SequenceOption<ScalarOption<float>>*>(targetConfig.getOptions()[i]);
            EXPECT_EQ(sourceOption->size(), targetOption->size());
            for (size_t j = 0; j < sourceOption->size(); ++j)
            {
                std::cout << "Target Float: " << targetOption->operator[](j).getValue() << " "
                          << "Source Float: " << sourceOption->operator[](j).getValue() << std::endl;
                EXPECT_NE(sourceOption->operator[](j).getValue(), targetOption->operator[](j).getValue());
            }
        }
        else if (auto* sourceOption = dynamic_cast<SequenceOption<ScalarOption<bool>>*>(sourceConfig.getOptions()[i]))
        {
            std::cout << "Bool Sequences" << std::endl;
            auto* targetOption = dynamic_cast<SequenceOption<ScalarOption<bool>>*>(targetConfig.getOptions()[i]);
            EXPECT_EQ(sourceOption->size(), targetOption->size());
            for (size_t j = 0; j < sourceOption->size(); ++j)
            {
                std::cout << "Target Bool: " << targetOption->operator[](j).getValue() << " "
                          << "Source Bool: " << sourceOption->operator[](j).getValue() << std::endl;
                EXPECT_NE(sourceOption->operator[](j).getValue(), targetOption->operator[](j).getValue());
            }
        }
        else if (auto* sourceOption = dynamic_cast<SequenceOption<ScalarOption<uint64_t>>*>(sourceConfig.getOptions()[i]))
        {
            std::cout << "Integer Sequences" << std::endl;
            auto* targetOption = dynamic_cast<SequenceOption<ScalarOption<uint64_t>>*>(targetConfig.getOptions()[i]);
            EXPECT_EQ(sourceOption->size(), targetOption->size());
            for (size_t j = 0; j < sourceOption->size(); ++j)
            {
                std::cout << "Target Integer: " << targetOption->operator[](j).getValue() << " "
                          << "Source Integer: " << sourceOption->operator[](j).getValue() << std::endl;
                EXPECT_NE(sourceOption->operator[](j).getValue(), targetOption->operator[](j).getValue());
            }
        }
    }

    targetConfig.accept(deserializer);
    std::cout << '\n' << "----- After overwriting -----" << '\n' << std::endl;
    for (size_t i = 0; i < sourceConfig.getOptions().size(); ++i)
    {
        if (auto* sourceOption = dynamic_cast<SequenceOption<ScalarOption<std::string>>*>(sourceConfig.getOptions()[i]))
        {
            std::cout << "String Sequences" << std::endl;
            auto* targetOption = dynamic_cast<SequenceOption<ScalarOption<std::string>>*>(targetConfig.getOptions()[i]);
            EXPECT_EQ(sourceOption->size(), targetOption->size());
            for (size_t j = 0; j < sourceOption->size(); ++j)
            {
                std::cout << "Target String: '" << targetOption->operator[](j).getValue() << "' "
                          << "Source String: '" << sourceOption->operator[](j).getValue() << "'" << std::endl;
                EXPECT_EQ(sourceOption->operator[](j).getValue(), targetOption->operator[](j).getValue());
            }
        }
        else if (auto* sourceOption = dynamic_cast<SequenceOption<ScalarOption<float>>*>(sourceConfig.getOptions()[i]))
        {
            std::cout << "Float Sequences" << std::endl;
            auto* targetOption = dynamic_cast<SequenceOption<ScalarOption<float>>*>(targetConfig.getOptions()[i]);
            EXPECT_EQ(sourceOption->size(), targetOption->size());
            for (size_t j = 0; j < sourceOption->size(); ++j)
            {
                std::cout << "Target Float: " << targetOption->operator[](j).getValue() << " "
                          << "Source Float: " << sourceOption->operator[](j).getValue() << std::endl;
                EXPECT_EQ(sourceOption->operator[](j).getValue(), targetOption->operator[](j).getValue());
            }
        }
        else if (auto* sourceOption = dynamic_cast<SequenceOption<ScalarOption<bool>>*>(sourceConfig.getOptions()[i]))
        {
            std::cout << "Bool Sequences" << std::endl;
            auto* targetOption = dynamic_cast<SequenceOption<ScalarOption<bool>>*>(targetConfig.getOptions()[i]);
            EXPECT_EQ(sourceOption->size(), targetOption->size());
            for (size_t j = 0; j < sourceOption->size(); ++j)
            {
                std::cout << "Target Bool: " << targetOption->operator[](j).getValue() << " "
                          << "Source Bool: " << sourceOption->operator[](j).getValue() << std::endl;
                EXPECT_EQ(sourceOption->operator[](j).getValue(), targetOption->operator[](j).getValue());
            }
        }
        else if (auto* sourceOption = dynamic_cast<SequenceOption<ScalarOption<uint64_t>>*>(sourceConfig.getOptions()[i]))
        {
            std::cout << "Integer Sequences" << std::endl;
            auto* targetOption = dynamic_cast<SequenceOption<ScalarOption<uint64_t>>*>(targetConfig.getOptions()[i]);
            EXPECT_EQ(sourceOption->size(), targetOption->size());
            for (size_t j = 0; j < sourceOption->size(); ++j)
            {
                std::cout << "Target Integer: " << targetOption->operator[](j).getValue() << " "
                          << "Source Integer: " << sourceOption->operator[](j).getValue() << std::endl;
                EXPECT_EQ(sourceOption->operator[](j).getValue(), targetOption->operator[](j).getValue());
            }
        }
    }
}

TEST_F(ConfigSerializationTest, SerializationTest)
{
    ProtobufConfigCache cache;

    /// Source configuration (before serialization)
    WorkerConfiguration sourceConfiguration("source configuration", "Values to be serialized.");

    // Modify source configuration so it does not contain the default values
    sourceConfiguration.localWorkerHost.setValue("0.0.0.0");
    sourceConfiguration.rpcPort.setValue(10);
    sourceConfiguration.numberOfBuffersInGlobalBufferManager.setValue(98765);
    sourceConfiguration.numberOfBuffersInSourceLocalBufferPool.setValue(128);
    sourceConfiguration.bufferSizeInBytes.setValue(42);
    sourceConfiguration.logLevel.setValue(LogLevel::LOG_NONE);
    sourceConfiguration.configPath.setValue("THE PATH");

    sourceConfiguration.queryCompiler.compilationStrategy.setValue(QueryCompilation::CompilationStrategy::FAST);
    sourceConfiguration.queryCompiler.nautilusBackend.setValue(QueryCompilation::NautilusBackend::INTERPRETER);
    sourceConfiguration.queryCompiler.useCompilationCache.setValue(true);
    sourceConfiguration.queryCompiler.numberOfPartitions.setValue(3);
    sourceConfiguration.queryCompiler.pageSize.setValue(50);
    sourceConfiguration.queryCompiler.preAllocPageCnt.setValue(4);
    sourceConfiguration.queryCompiler.maxHashTableSize.setValue(7);

    // Serialize source worker configuration
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
    std::cout << sourceConfig.a.getName() << std::endl;

    /// Modify configuration so it does not contain the default values
    sourceConfig.a.setValue(-52);
    sourceConfig.b.setValue(2.35);

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

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

#include <Configurations/PrintingVisitor.hpp>
#include <Configurations/ProtobufConfigCache.hpp>
#include <Configurations/ProtobufDeserializeVisitor.hpp>
#include <Configurations/ProtobufSerializeOptionVisitor.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Util/Logger/Logger.hpp>

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
        NES_INFO("Setup Configuration test class.");
    }
};

class TestConfiguration : public BaseConfiguration
{
public:
    TestConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description)
    {
        sequenceOfStrings.add("Are");
        sequenceOfStrings.add("we");
        sequenceOfStrings.add("testing");
        sequenceOfStrings.add("correctly");
        sequenceOfStrings.add("?");

        sequenceOfIntegers.add(1);
        sequenceOfIntegers.add(2);
        sequenceOfIntegers.add(3);
        sequenceOfIntegers.add(4);
        sequenceOfIntegers.add(5);

        sequenceOfFloats.add(1.1);
        sequenceOfFloats.add(1.2);
        sequenceOfFloats.add(1.3);
        sequenceOfFloats.add(1.4);
        sequenceOfFloats.add(1.5);

        sequenceOfBooleans.add(true);
        sequenceOfBooleans.add(false);
        sequenceOfBooleans.add(true);
        sequenceOfBooleans.add(false);
        sequenceOfBooleans.add(true);
    }
    TestConfiguration() = default;

    SequenceOption<ScalarOption<std::string>> sequenceOfStrings = {"Strings", "A sequence of strings."};
    SequenceOption<ScalarOption<float>> sequenceOfFloats = {"Floats", "A sequence of floats."};
    SequenceOption<ScalarOption<uint64_t>> sequenceOfIntegers = {"Integers", "A sequence of integers."};
    SequenceOption<ScalarOption<bool>> sequenceOfBooleans = {"Booleans", "A sequence of booleans."};

protected:
    std::vector<BaseOption*> getOptions() override
    {
        return {&sequenceOfStrings, &sequenceOfFloats, &sequenceOfIntegers, &sequenceOfBooleans};
    }
};

TEST_F(ConfigSerializationTest, seri)
{
    ProtobufConfigCache cache;

    /// First configuration
    WorkerConfiguration configuration("root", "");

    /// Modify configuration so it does not contain the default values
    configuration.localWorkerHost.setValue("0.0.0.0");
    configuration.rpcPort.setValue(10);
    configuration.numberOfWorkerThreads.setValue(12345);
    configuration.numberOfBuffersInGlobalBufferManager.setValue(98765);
    configuration.numberOfBuffersInSourceLocalBufferPool.setValue(128);
    configuration.bufferSizeInBytes.setValue(42);
    configuration.logLevel.setValue(LogLevel::LOG_NONE);
    configuration.configPath.setValue("THE PATH");

    configuration.queryCompiler.compilationStrategy.setValue(QueryCompilation::CompilationStrategy::FAST);
    configuration.queryCompiler.nautilusBackend.setValue(QueryCompilation::NautilusBackend::INTERPRETER);
    configuration.queryCompiler.useCompilationCache.setValue(true);
    configuration.queryCompiler.numberOfPartitions.setValue(3);
    configuration.queryCompiler.pageSize.setValue(50);
    configuration.queryCompiler.preAllocPageCnt.setValue(4);
    configuration.queryCompiler.maxHashTableSize.setValue(7);

    /// Serialize configuration
    auto* message = cache.getEmptyMessage<WorkerConfiguration>();
    ProtobufSerializeOptionVisitor serializer(message);
    configuration.accept(serializer);

    /// Second configuration with default values
    WorkerConfiguration receiveConfiguration("root", "");
    PrintingVisitor printingVisitor(std::cout);
    receiveConfiguration.accept(printingVisitor);

    std::cout << "\n\n";

    /// Deserialize message overwriting default configuration
    ProtobufDeserializeVisitor deserializer(message);
    EXPECT_NE(configuration.toString(), receiveConfiguration.toString());
    receiveConfiguration.accept(deserializer);
    EXPECT_EQ(configuration.toString(), receiveConfiguration.toString());
    receiveConfiguration.accept(printingVisitor);
}

/// TODO #429: Serialization/Deserialization of SequenceOption doesn't work (PrintingVisitor does)
TEST_F(ConfigSerializationTest, SequenceOptionTest)
{ /*
    ProtobufConfigCache cache;

    /// First Configuration
    TestConfiguration configuration = {"TestSequenceConfig", "config with sequences."};

    /// Modify configuration so it does not contain the default values
    configuration.sequenceOfStrings.add("We");
    configuration.sequenceOfStrings.add("are");
    configuration.sequenceOfStrings.add("correctly");
    configuration.sequenceOfStrings.add("testing");
    configuration.sequenceOfStrings.add("!");

    configuration.sequenceOfIntegers.add(10);
    configuration.sequenceOfIntegers.add(9);
    configuration.sequenceOfIntegers.add(8);
    configuration.sequenceOfIntegers.add(7);
    configuration.sequenceOfIntegers.add(6);

    configuration.sequenceOfFloats.add(2.1);
    configuration.sequenceOfFloats.add(3.1);
    configuration.sequenceOfFloats.add(4.1);
    configuration.sequenceOfFloats.add(5.1);
    configuration.sequenceOfFloats.add(6.1);

    configuration.sequenceOfBooleans.add(false);
    configuration.sequenceOfBooleans.add(true);
    configuration.sequenceOfBooleans.add(false);
    configuration.sequenceOfBooleans.add(true);
    configuration.sequenceOfBooleans.add(false);

    /// Serialize configuration
    auto* message = cache.getEmptyMessage<TestConfiguration>();
    ProtobufSerializeOptionVisitor serializer(message);
    configuration.accept(serializer);

    /// Second configuration with default values
    TestConfiguration receiveConfiguration("root", "");
    PrintingVisitor printingVisitor(std::cout);
    receiveConfiguration.accept(printingVisitor);

    std::cout << '\n';

    /// Deserialize message overwriting default configuration
    ProtobufDeserializeVisitor deserializer(message);
    EXPECT_NE(configuration.toString(), receiveConfiguration.toString());
    receiveConfiguration.accept(deserializer);
    EXPECT_EQ(configuration.toString(), receiveConfiguration.toString());
    receiveConfiguration.accept(printingVisitor);*/
}

}


#include "SetsumSink.hpp"
#include <barrier>
#include <Configurations/Descriptor.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinkRegistry.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <cstddef>
#include <thread>
#include <vector>
#include <gtest/gtest.h>
#include <DataTypes/Schema.hpp>
#include <DataTypes/DataType.hpp>
#include <Runtime/BufferManager.hpp>
#include <PipelineExecutionContext.hpp>
#include <TestTaskQueue.hpp>

namespace NES
{
TEST(SetsumSinkTest, VerifyAccumulation) {

    // 1. Create a configuration map
    std::unordered_map<std::string, std::string> config;
    config[SinkDescriptor::FILE_PATH] = "test_output.csv";

    auto catalog = std::make_shared<SinkCatalog>();
    auto schema = std::make_shared<Schema>();
    schema->addField("id", DataType::Type::UINT32);

    // auto descriptorOpt = catalog->addSinkDescriptor("my_setsum_sink", *schema, "Setsum", config);
    // auto DescriptorConfig = SinkDescriptor::validateAndFormatConfig("Setsum", config);

    auto descriptorOpt = catalog->getInlineSink(*schema, "Setsum", config);

    ASSERT_TRUE(descriptorOpt.has_value()) << "Failed to create inline sink descriptor due to validation error.";

    // 5. Package into Registry Arguments
    NES::SinkRegistryArguments args{descriptorOpt.value()};
    args.sinkDescriptor = descriptorOpt.value();

    // 6. Instantiate the Sink using the static member function
    auto sink = SetsumSink::RegisterSetsumSink(args);
    auto setsumSink = static_cast<SetsumSink*>(sink.get());
    ASSERT_NE(sink, nullptr);

    auto bufferManager = BufferManager::create();

    auto resultBuffers = std::make_shared<std::vector<std::vector<TupleBuffer>>>(1);

    // 3. Instantiate the TestPipelineExecutionContext
    // Using the second explicit constructor: (bufferProvider, resultBufferPtr)
    auto context = std::make_shared<TestPipelineExecutionContext>(bufferManager, resultBuffers);

    setsumSink->start(context);
    // Feed Tuples
    setsumSink->execute("apple",context);
    setsumSink->execute("banana",context);

    // Verify Internal State
    Setsum expected;
    expected.add("apple");
    expected.add("banana");

    EXPECT_EQ(setsumSink->getCurrentSetsum(), expected);
}
}
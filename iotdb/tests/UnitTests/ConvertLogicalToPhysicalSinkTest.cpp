#include "gtest/gtest.h"
#include <API/Schema.hpp>
#include <SourceSink/FileOutputSink.hpp>
#include <Nodes/Phases/ConvertLogicalToPhysicalSink.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Util/Logger.hpp>

namespace NES {
class ConvertLogicalToPhysicalSinkTest : public testing::Test {
  public:

    static void SetUpTestCase() {
        NES::setupLogging("ConvertLogicalToPhysicalSinkTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup ConvertLogicalToPhysicalSinkTest test class.");
    }

    static void TearDownTestCase() {
        std::cout << "Tear down ConvertLogicalToPhysicalSinkTest test class." << std::endl;
    }

    void TearDown() {
    }
};

TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingFileLogiclaToPhysicalSink) {

    SchemaPtr schema = Schema::create();
    SinkDescriptorPtr sinkDescriptor = std::make_shared<FileSinkDescriptor>(schema,
                                                                            "file.log",
                                                                            FileOutPutMode::FILE_OVERWRITE,
                                                                            FileOutPutType::CSV_TYPE);
    DataSinkPtr fileOutputSink = ConvertLogicalToPhysicalSink::createDataSink(sinkDescriptor);
    EXPECT_EQ(fileOutputSink->getType(), FILE_SINK);
}

}


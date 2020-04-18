#include "gtest/gtest.h"
#include <API/Schema.hpp>
#include <Nodes/Phases/ConvertLogicalToPhysicalSource.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Util/Logger.hpp>

namespace NES {
class ConvertLogicalToPhysicalSinkTest : public testing::Test {
  public:

    static void SetUpTestCase() {
        NES::setupLogging("ConvertLogicalToPhysicalSourceTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup ConvertLogicalToPhysicalSourceTest test class.");
    }

    static void TearDownTestCase() {
        std::cout << "Tear down ConvertLogicalToPhysicalSourceTest test class." << std::endl;
    }

    void TearDown() {
    }
};

TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingCsvFileLogiclaToPhysicalSource) {

    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = std::make_shared<CsvSourceDescriptor>(schema, "file.log", ",", 10, 10);
    DataSourcePtr csvFileSource = ConvertLogicalToPhysicalSource::createDataSource(sourceDescriptor);
    EXPECT_EQ(csvFileSource->getType(), CSV_SOURCE);
}

}


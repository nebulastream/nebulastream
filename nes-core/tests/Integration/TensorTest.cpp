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

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-copy-dtor"
#include "../util/NesBaseTest.hpp"
#include <NesBaseTest.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#pragma clang diagnostic pop
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/ExecutableType/Array.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

class TensorTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("JoinDeploymentTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup JoinDeploymentTest test class.");
    }
};

/**
* Test deploying join with same data and same schema
 * */
TEST_F(TensorTest, testCreateVectorInSchema) {

    struct Vector {
        double durationMonth;
        double creditAmount;
        double score;
        double age25;
        double durationMonth_1;
        double creditAmount_1;
        double score_1;
        double age25_1;

        // overload the == operator to check if two instances are the same
        bool operator==(Vector const& rhs) const {
            return (durationMonth == rhs.durationMonth && creditAmount == rhs.creditAmount && score == rhs.score && age25 == rhs.age25 && durationMonth_1 == rhs.durationMonth_1
                    && creditAmount_1 == rhs.creditAmount_1 && score_1 == rhs.score_1 && age25_1 == rhs.age25_1);
        }
    };

    std::vector<std::size_t> shape;
    shape.push_back(4);

    auto tensorSchema = Schema::create()
                            ->addField("vector1", DataTypeFactory::createTensor(shape, "FLOAT64", "DENSE"))
                            ->addField("vector2", DataTypeFactory::createTensor(shape, "FLOAT64", "DENSE"));

    auto test = tensorSchema->getSchemaSizeInBytes();

    ASSERT_EQ(sizeof(Vector), test);

    auto csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath(std::string(TEST_DATA_DIRECTORY) + "GermanCreditAge25short.csv");
    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType->setNumberOfBuffersToProduce(1);
    csvSourceType->setSkipHeader(true);

    std::string outputFilePath = getTestResourceFolder() / "tensorTest.out";

    string query = R"(Query::from("tensorTest"))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("tensorTest", tensorSchema)
                                  .attachWorkerWithCSVSourceToCoordinator("tensorTest", csvSourceType)
                                  .validate()
                                  .setupTopology();

    std::vector<Vector> expectedOutput = {{0.029411765, 0.949433256, 0.600715353, 0, 0.029411765, 0.949433256, 0.600715353, 0},
                                          {0.647058824, 0.686310113, 0.496130655, 1, 0.647058824, 0.686310113, 0.496130655, 1},
                                          {0.117647059, 0.898426323, 0.506866366, 0, 0.117647059, 0.898426323, 0.506866366, 0},
                                          {0.558823529, 0.580059426, 0.5994821, 0, 0.558823529, 0.580059426, 0.5994821, 0},
                                          {0.294117647, 0.74579069, 0.550060466, 0, 0.294117647, 0.74579069, 0.550060466, 0},
                                          {0.470588235, 0.515516672, 0.501290835, 0, 0.470588235, 0.515516672, 0.501290835, 0},
                                          {0.294117647, 0.857763838, 0.617404307, 0, 0.294117647, 0.857763838, 0.617404307, 0},
                                          {0.470588235, 0.631451524, 0.476348482, 0, 0.470588235, 0.631451524, 0.476348482, 0},
                                          {0.117647059, 0.845438539, 0.543519801, 0, 0.117647059, 0.845438539, 0.543519801, 0},
                                          {0.382352941, 0.725762078, 0.368951631, 0, 0.382352941, 0.725762078, 0.368951631, 0}};

    std::vector<Vector> actualOutput = testHarness.getOutput<Vector>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, expectedOutput);
}
/**
* Test deploying join with same data and same schema
 * */
TEST_F(TensorTest, testCreateMatrixInSchema) {

    struct Matrix {
        double durationMonth;
        double creditAmount;
        double score;
        double age25;
        double durationMonth_1;
        double creditAmount_1;
        double score_1;
        double age25_1;

        // overload the == operator to check if two instances are the same
        bool operator==(Matrix const& rhs) const {
            return (durationMonth == rhs.durationMonth && creditAmount == rhs.creditAmount && score == rhs.score && age25 == rhs.age25 && durationMonth_1 == rhs.durationMonth_1
                    && creditAmount_1 == rhs.creditAmount_1 && score_1 == rhs.score_1 && age25_1 == rhs.age25_1);
        }
    };

    std::vector<std::size_t> shape;
    shape.push_back(2);
    shape.push_back(2);

    auto tensorSchema = Schema::create()
                            ->addField("matrix1", DataTypeFactory::createTensor(shape, "FLOAT64", "DENSE"))
                            ->addField("matrix2", DataTypeFactory::createTensor(shape, "FLOAT64", "DENSE"));

    auto test = tensorSchema->getSchemaSizeInBytes();

    ASSERT_EQ(sizeof(Matrix), test);

    auto csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath(std::string(TEST_DATA_DIRECTORY) + "GermanCreditAge25short.csv");
    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType->setNumberOfBuffersToProduce(1);
    csvSourceType->setSkipHeader(true);

    std::string outputFilePath = getTestResourceFolder() / "tensorTest.out";

    string query = R"(Query::from("tensorTest"))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("tensorTest", tensorSchema)
                                  .attachWorkerWithCSVSourceToCoordinator("tensorTest", csvSourceType)
                                  .validate()
                                  .setupTopology();

    std::vector<Matrix> expectedOutput = {{0.029411765, 0.949433256, 0.600715353, 0, 0.029411765, 0.949433256, 0.600715353, 0},
                                          {0.647058824, 0.686310113, 0.496130655, 1, 0.647058824, 0.686310113, 0.496130655, 1},
                                          {0.117647059, 0.898426323, 0.506866366, 0, 0.117647059, 0.898426323, 0.506866366, 0},
                                          {0.558823529, 0.580059426, 0.5994821, 0, 0.558823529, 0.580059426, 0.5994821, 0},
                                          {0.294117647, 0.74579069, 0.550060466, 0, 0.294117647, 0.74579069, 0.550060466, 0},
                                          {0.470588235, 0.515516672, 0.501290835, 0, 0.470588235, 0.515516672, 0.501290835, 0},
                                          {0.294117647, 0.857763838, 0.617404307, 0, 0.294117647, 0.857763838, 0.617404307, 0},
                                          {0.470588235, 0.631451524, 0.476348482, 0, 0.470588235, 0.631451524, 0.476348482, 0},
                                          {0.117647059, 0.845438539, 0.543519801, 0, 0.117647059, 0.845438539, 0.543519801, 0},
                                          {0.382352941, 0.725762078, 0.368951631, 0, 0.382352941, 0.725762078, 0.368951631, 0}};

    std::vector<Matrix> actualOutput = testHarness.getOutput<Matrix>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
* Test deploying join with same data and same schema
 * */
TEST_F(TensorTest, testCreateCubeInSchema) {

    struct Cube {
        double durationMonth;
        double creditAmount;
        double score;
        double age25;
        double durationMonth_1;
        double creditAmount_1;
        double score_1;
        double age25_1;

        // overload the == operator to check if two instances are the same
        bool operator==(Cube const& rhs) const {
            return (durationMonth == rhs.durationMonth && creditAmount == rhs.creditAmount && score == rhs.score && age25 == rhs.age25 && durationMonth_1 == rhs.durationMonth_1
                    && creditAmount_1 == rhs.creditAmount_1 && score_1 == rhs.score_1 && age25_1 == rhs.age25_1);
        }
    };

    std::vector<std::size_t> shape;
    shape.push_back(2);
    shape.push_back(2);
    shape.push_back(2);

    auto tensorSchema = Schema::create()->addField("cube", DataTypeFactory::createTensor(shape, "FLOAT64", "DENSE"));

    auto test = tensorSchema->getSchemaSizeInBytes();

    ASSERT_EQ(sizeof(Cube), test);

    auto csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath(std::string(TEST_DATA_DIRECTORY) + "GermanCreditAge25short.csv");
    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType->setNumberOfBuffersToProduce(1);
    csvSourceType->setSkipHeader(true);

    std::string outputFilePath = getTestResourceFolder() / "tensorTest.out";

    string query = R"(Query::from("tensorTest"))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("tensorTest", tensorSchema)
                                  .attachWorkerWithCSVSourceToCoordinator("tensorTest", csvSourceType)
                                  .validate()
                                  .setupTopology();

    struct Output {
        int64_t start;
        int64_t end;
        int64_t key;
        uint64_t value1;
        uint64_t id1;
        uint64_t timestamp1;
        uint64_t value2;
        uint64_t id2;
        uint64_t timestamp2;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && key == rhs.key && value1 == rhs.value1 && id1 == rhs.id1
                    && timestamp1 == rhs.timestamp1 && value2 == rhs.value2 && id2 == rhs.id2 && timestamp2 == rhs.timestamp2);
        }
    };

    std::vector<Cube> expectedOutput = {{0.029411765, 0.949433256, 0.600715353, 0, 0.029411765, 0.949433256, 0.600715353, 0},
                                        {0.647058824, 0.686310113, 0.496130655, 1, 0.647058824, 0.686310113, 0.496130655, 1},
                                        {0.117647059, 0.898426323, 0.506866366, 0, 0.117647059, 0.898426323, 0.506866366, 0},
                                        {0.558823529, 0.580059426, 0.5994821, 0, 0.558823529, 0.580059426, 0.5994821, 0},
                                        {0.294117647, 0.74579069, 0.550060466, 0, 0.294117647, 0.74579069, 0.550060466, 0},
                                        {0.470588235, 0.515516672, 0.501290835, 0, 0.470588235, 0.515516672, 0.501290835, 0},
                                        {0.294117647, 0.857763838, 0.617404307, 0, 0.294117647, 0.857763838, 0.617404307, 0},
                                        {0.470588235, 0.631451524, 0.476348482, 0, 0.470588235, 0.631451524, 0.476348482, 0},
                                        {0.117647059, 0.845438539, 0.543519801, 0, 0.117647059, 0.845438539, 0.543519801, 0},
                                        {0.382352941, 0.725762078, 0.368951631, 0, 0.382352941, 0.725762078, 0.368951631, 0}};

    std::vector<Cube> actualOutput = testHarness.getOutput<Cube>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
* Test deploying join with same data and same schema
 * */
TEST_F(TensorTest, testCreateVectorSimpleFieldsMatrixInSchema) {

    //TODO: tell ankit and philipp that int8_t and uint8_t take up more space in c++ than in nes
    // size of schema with vector and matrix 48
    // size of schema with vector, matrix and int8 49 in nes, in c++ it's 56
    // uint16_t not same size either: c++ 64, nes 62 same for int16_t
    // 32 is same size
    struct TestSchema {
        double durationMonth;
        double creditAmount;
        std::array<char, 12> score;
        uint32_t age25;
        double durationMonth_1;
        double creditAmount_1;
        double score_1;
        double age25_1;

        // overload the == operator to check if two instances are the same
        bool operator==(TestSchema const& rhs) const {
            return (durationMonth == rhs.durationMonth && creditAmount == rhs.creditAmount && score == rhs.score && age25 == rhs.age25 && durationMonth_1 == rhs.durationMonth_1
                    && creditAmount_1 == rhs.creditAmount_1 && score_1 == rhs.score_1 && age25_1 == rhs.age25_1);
        }
    };

    std::vector<std::size_t> vectorShape;
    vectorShape.push_back(2);
    std::vector<std::size_t> matrixShape;
    matrixShape.push_back(2);
    matrixShape.push_back(2);

    auto tensorSchema = Schema::create()
                            ->addField("vector", DataTypeFactory::createTensor(vectorShape, "FLOAT64", "DENSE"))
                            ->addField("score", DataTypeFactory::createArray(12, DataTypeFactory::createChar()))
                            ->addField("age25", UINT32)
                            ->addField("matrix", DataTypeFactory::createTensor(matrixShape, "FLOAT64", "DENSE"));

    auto test = tensorSchema->getSchemaSizeInBytes();

    ASSERT_EQ(sizeof(TestSchema), test);

    auto csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath(std::string(TEST_DATA_DIRECTORY) + "GermanCreditAge25short.csv");
    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType->setNumberOfBuffersToProduce(1);
    csvSourceType->setSkipHeader(true);

    std::string outputFilePath = getTestResourceFolder() / "tensorTest.out";

    string query = R"(Query::from("tensorTest"))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("tensorTest", tensorSchema)
                                  .attachWorkerWithCSVSourceToCoordinator("tensorTest", csvSourceType)
                                  .validate()
                                  .setupTopology();

    std::vector<TestSchema> expectedOutput = {{0.029411765, 0.949433256, {"0.600715353"}, 0, 0.029411765, 0.949433256, 0.600715353, 0},
                                          {0.647058824, 0.686310113, {"0.496130655"}, 1, 0.647058824, 0.686310113, 0.496130655, 1},
                                          {0.117647059, 0.898426323, {"0.506866366"}, 0, 0.117647059, 0.898426323, 0.506866366, 0},
                                          {0.558823529, 0.580059426, {"0.5994821"}, 0, 0.558823529, 0.580059426, 0.5994821, 0},
                                          {0.294117647, 0.74579069, {"0.550060466"}, 0, 0.294117647, 0.74579069, 0.550060466, 0},
                                          {0.470588235, 0.515516672, {"0.501290835"}, 0, 0.470588235, 0.515516672, 0.501290835, 0},
                                          {0.294117647, 0.857763838, {"0.617404307"}, 0, 0.294117647, 0.857763838, 0.617404307, 0},
                                          {0.470588235, 0.631451524, {"0.476348482"}, 0, 0.470588235, 0.631451524, 0.476348482, 0},
                                          {0.117647059, 0.845438539, {"0.543519801"}, 0, 0.117647059, 0.845438539, 0.543519801, 0},
                                          {0.382352941, 0.725762078, {"0.368951631"}, 0, 0.382352941, 0.725762078, 0.368951631, 0}};


    std::vector<TestSchema> actualOutput = testHarness.getOutput<TestSchema>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

}// namespace NES
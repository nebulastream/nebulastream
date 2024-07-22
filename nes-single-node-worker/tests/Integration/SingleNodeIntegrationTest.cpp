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

#include <TestUtils/UtilityFunctions.hpp>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <numeric>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/Serialization/OperatorSerializationUtil.hpp>
#include <Operators/Serialization/SchemaSerializationUtil.hpp>
#include <BaseIntegrationTest.hpp>
#include <GrpcService.hpp>
#include <SingleNodeWorkerRPCService.pb.h>

namespace NES::Testing
{
using namespace ::testing;
/**
 * @brief Integration tests for the SingleNodeWorker. Tests entire code path from registration to running the query, stopping and
 * unregistration.
 */
class SingleNodeIntegrationTest : public BaseIntegrationTest
{
public:
    static void SetUpTestCase()
    {
        BaseIntegrationTest::SetUpTestCase();
        Logger::setupLogging("SingleNodeIntegrationTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup SingleNodeIntegrationTest test class.");
    }

    std::filesystem::path redirectOutput(SerializableDecomposedQueryPlan& queryPlan) const
    {
        EXPECT_EQ(queryPlan.mutable_rootoperatorids()->size(), 1) << "Redirection is only implemented for Single Sink Queries";

        const auto rootOperatorId = (queryPlan).mutable_rootoperatorids()->at(0);
        auto& rootOperator = (queryPlan).mutable_operatormap()->at(rootOperatorId);
        EXPECT_TRUE(rootOperator.details().Is<SerializableOperator_SinkDetails>())
            << "Redirection expects the single root operator to be a sink operator";

        auto filename = (getTestResourceFolder() / "filesinkoutputXXXXXX").string();
        mkstemp(filename.data());
        auto details = SerializableOperator_SinkDetails();
        const auto printSink = FileSinkDescriptor::create(filename);
        OperatorSerializationUtil::serializeSinkDescriptor(*printSink, details, 1);
        rootOperator.mutable_details()->PackFrom(details);

        return filename;
    }
};

/**
 * Loads a protobuf serialized @link SerializableDecomposedQueryPlan from a file in the TEST_DATA_DIR. This is a macro because
 * it will skip the test if the query does not exist.
 * @param ptr Mutable ptr to a @link SerializableDecomposedQueryPlan
 * @param filename name of the file in the TEST_DATA_DIR
 */
#define LOAD(ptr, filename) \
    do \
    { \
        std::ifstream f(std::filesystem::path(TEST_DATA_DIR) / (filename)); \
        if (!f) \
        { \
            GTEST_SKIP_("Test Data is not available: " TEST_DATA_DIR "/" filename); \
        } \
        if (!(ptr)->ParseFromIstream(&f)) \
        { \
            GTEST_SKIP_("Could not load protobuffer file: " TEST_DATA_DIR "/" filename); \
        } \
    } while (0)

/**
 * Loads the output @link Schema of the SinkOperator in the @link SerializableDecomposedQueryPlan. This requieres the plan to only
 * have a single root operator, which is the SinkOperator
 * @param plan
 * @return output @link Schema
 */
SchemaPtr loadSinkSchema(SerializableDecomposedQueryPlan& plan)
{
    EXPECT_EQ(plan.mutable_rootoperatorids()->size(), 1) << "Redirection is only implemented for Single Sink Queries";
    const auto rootOperatorId = plan.mutable_rootoperatorids()->at(0);
    auto& rootOperator = plan.mutable_operatormap()->at(rootOperatorId);
    EXPECT_TRUE(rootOperator.details().Is<SerializableOperator_SinkDetails>())
        << "Redirection expects the single root operator to be a sink operator";
    return SchemaSerializationUtil::deserializeSchema(rootOperator.outputschema());
}

QueryId registerQueryPlan(SerializableDecomposedQueryPlan& plan, GRPCServer& uut)
{
    grpc::ServerContext context;
    RegisterQueryRequest request;
    RegisterQueryReply reply;
    *request.mutable_decomposedqueryplan() = plan;
    EXPECT_TRUE(uut.RegisterQuery(&context, &request, &reply).ok());
    return QueryId(reply.queryid());
}

void startQuery(QueryId queryId, GRPCServer& uut)
{
    grpc::ServerContext context;
    StartQueryRequest request;
    google::protobuf::Empty reply;
    request.set_queryid(queryId.getRawValue());
    EXPECT_TRUE(uut.StartQuery(&context, &request, &reply).ok());
}

void stopQuery(QueryId queryId, QueryTerminationType type, GRPCServer& uut)
{
    grpc::ServerContext context;
    StopQueryRequest request;
    google::protobuf::Empty reply;
    request.set_queryid(queryId.getRawValue());
    request.set_terminationtype(type);
    EXPECT_TRUE(uut.StopQuery(&context, &request, &reply).ok());
}

void unregisterQuery(QueryId queryId, GRPCServer& uut)
{
    grpc::ServerContext context;
    UnregisterQueryRequest request;
    google::protobuf::Empty reply;
    request.set_queryid(queryId.getRawValue());
    EXPECT_TRUE(uut.UnregisterQuery(&context, &request, &reply).ok());
}

TEST_F(SingleNodeIntegrationTest, TestQueryRegistration)
{
    SerializableDecomposedQueryPlan queryPlan;
    LOAD(&queryPlan, "query_single_csv_source.bin");
    auto outputFile = redirectOutput(queryPlan);

    Configuration::SingleNodeWorkerConfiguration configuration{};
    configuration.queryCompilerConfiguration.nautilusBackend = QueryCompilation::NautilusBackend::MLIR_COMPILER_BACKEND;

    GRPCServer uut{SingleNodeWorker{configuration}};

    auto queryId = registerQueryPlan(queryPlan, uut);
    startQuery(queryId, uut);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    stopQuery(queryId, HardStop, uut);
    unregisterQuery(queryId, uut);

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>();
    auto buffers
        = Runtime::Execution::Util::createBuffersFromCSVFile(outputFile.string(), loadSinkSchema(queryPlan), bufferManager, 0, "", true);

    auto numberOfTuples
        = std::accumulate(buffers.begin(), buffers.end(), 0, [](auto acc, const auto& buffer) { return acc + buffer.getNumberOfTuples(); });

    // A single CSV source producing 32 tuples.
    constexpr auto EXPECTED_NUMBER_OF_TUPLES = 32;
    EXPECT_EQ(numberOfTuples, EXPECTED_NUMBER_OF_TUPLES) << "Query did not produce the expected number of tuples";
}
} // namespace NES::Testing

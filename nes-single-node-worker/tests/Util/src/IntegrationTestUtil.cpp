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

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/TCPSourceDescriptor.hpp>
#include <Operators/Serialization/OperatorSerializationUtil.hpp>
#include <Operators/Serialization/SchemaSerializationUtil.hpp>
#include <fmt/core.h>
#include <gtest/gtest.h>
#include <GrpcService.hpp>
#include <IntegrationTestUtil.hpp>
#include <SingleNodeWorkerRPCService.pb.h>

namespace NES
{
SchemaPtr IntegrationTestUtil::loadSinkSchema(SerializableDecomposedQueryPlan& queryPlan)
{
    EXPECT_EQ(queryPlan.mutable_rootoperatorids()->size(), 1) << "Redirection is only implemented for Single Sink Queries";
    const auto rootOperatorId = queryPlan.mutable_rootoperatorids()->at(0);
    auto& rootOperator = queryPlan.mutable_operatormap()->at(rootOperatorId);
    EXPECT_TRUE(rootOperator.details().Is<SerializableOperator_SinkDetails>())
        << "Redirection expects the single root operator to be a sink operator";
    return SchemaSerializationUtil::deserializeSchema(rootOperator.outputschema());
}

QueryId IntegrationTestUtil::registerQueryPlan(const SerializableDecomposedQueryPlan& queryPlan, GRPCServer& uut)
{
    grpc::ServerContext context;
    RegisterQueryRequest request;
    RegisterQueryReply reply;
    *request.mutable_decomposedqueryplan() = queryPlan;
    EXPECT_TRUE(uut.RegisterQuery(&context, &request, &reply).ok());
    return QueryId(reply.queryid());
}

void IntegrationTestUtil::startQuery(QueryId queryId, GRPCServer& uut)
{
    grpc::ServerContext context;
    StartQueryRequest request;
    google::protobuf::Empty reply;
    request.set_queryid(queryId.getRawValue());
    EXPECT_TRUE(uut.StartQuery(&context, &request, &reply).ok());
}

bool IntegrationTestUtil::isQueryFinished(QueryId queryId, GRPCServer& uut)
{
    grpc::ServerContext context;
    QueryStatusRequest request;
    QueryStatusReply reply;
    request.set_queryid(queryId.getRawValue());
    EXPECT_TRUE(uut.QueryStatus(&context, &request, &reply).ok());
    return reply.status() == QueryStatusReply::Finished;
}

void IntegrationTestUtil::stopQuery(QueryId queryId, Runtime::QueryTerminationType type, GRPCServer& uut)
{
    grpc::ServerContext context;
    StopQueryRequest request;
    google::protobuf::Empty reply;
    request.set_queryid(queryId.getRawValue());
    StopQueryRequest_QueryTerminationType typ{(int)type};
    request.set_terminationtype(typ);
    EXPECT_TRUE(uut.StopQuery(&context, &request, &reply).ok());
}

void IntegrationTestUtil::unregisterQuery(QueryId queryId, GRPCServer& uut)
{
    grpc::ServerContext context;
    UnregisterQueryRequest request;
    google::protobuf::Empty reply;
    request.set_queryid(queryId.getRawValue());
    EXPECT_TRUE(uut.UnregisterQuery(&context, &request, &reply).ok());
}

void IntegrationTestUtil::copyInputFile(const std::string_view inputFileName)
{
    try
    {
        const auto from = fmt::format("{}/{}/{}", TEST_DATA_DIR, INPUT_CSV_FILES, inputFileName);
        const auto to = fmt::format("./{}", inputFileName);
        copy_file(from, to, std::filesystem::copy_options::overwrite_existing);
    }
    catch (const std::filesystem::filesystem_error& e)
    {
        NES_ERROR("Error copying file: {}", e.what());
    }
}

void IntegrationTestUtil::removeFile(const std::string_view filepath)
{
    try
    {
        std::filesystem::remove(filepath);
    }
    catch (const std::filesystem::filesystem_error& e)
    {
        NES_ERROR("Error deleting file: {}", e.what());
    }
}

bool IntegrationTestUtil::loadFile(
    SerializableDecomposedQueryPlan& queryPlan, const std::filesystem::path& queryFile, const std::string_view dataFileName)
{
    std::ifstream file(queryFile);
    if (!file)
    {
        NES_ERROR("Query file is not available: {}", queryFile);
        return false;
    }
    if (!queryPlan.ParseFromIstream(&file))
    {
        NES_ERROR("Could not load protobuffer file: {}", queryFile);
        return false;
    }
    copyInputFile(dataFileName);
    return true;
}

bool IntegrationTestUtil::loadFile(SerializableDecomposedQueryPlan& queryPlan, const std::filesystem::path& queryFile)
{
    std::ifstream f(queryFile);
    if (!f)
    {
        NES_ERROR("Query file is not available: {}", queryFile);
        return false;
    }
    if (!queryPlan.ParseFromIstream(&f))
    {
        NES_ERROR("Could not load protobuffer file: {}", queryFile);
        return false;
    }
    return true;
}

void IntegrationTestUtil::replaceFileSinkPath(SerializableDecomposedQueryPlan& decomposedQueryPlan, const std::string& fileName)
{
    EXPECT_EQ(decomposedQueryPlan.mutable_rootoperatorids()->size(), 1) << "Redirection is only implemented for Single Sink Queries";
    const auto rootOperatorId = decomposedQueryPlan.mutable_rootoperatorids()->at(0);
    auto& rootOperator = decomposedQueryPlan.mutable_operatormap()->at(rootOperatorId);

    EXPECT_TRUE(rootOperator.details().Is<SerializableOperator_SinkDetails>())
        << "Redirection expects the single root operator to be a sink operator";
    const auto deserializedSinkperator = OperatorSerializationUtil::deserializeOperator(rootOperator);
    auto descriptor = deserializedSinkperator->as<SinkLogicalOperator>()->getSinkDescriptor()->as_if<FileSinkDescriptor>();
    if (descriptor)
    {
        descriptor->setFileName(fileName);
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(deserializedSinkperator);

        /// Reconfigure the original operator id, and childrenIds because deserialization/serialization changes them.
        serializedOperator.set_operatorid(rootOperator.operatorid());
        *serializedOperator.mutable_childrenids() = rootOperator.childrenids();

        swap(rootOperator, serializedOperator);
    }
}

void IntegrationTestUtil::replacePortInTcpSources(
    SerializableDecomposedQueryPlan& decomposedQueryPlan, const uint16_t mockTcpServerPort, const int sourceNumber)
{
    int queryPlanSourceTcpCounter = 0;
    for (auto& pair : *decomposedQueryPlan.mutable_operatormap())
    {
        google::protobuf::uint64 key = pair.first;
        auto& value = pair.second; /// Note: non-const reference
        if (value.details().Is<SerializableOperator_SourceDetails>())
        {
            auto deserializedSourceOperator = OperatorSerializationUtil::deserializeOperator(value);
            auto descriptor = deserializedSourceOperator->as<SourceLogicalOperator>()->getSourceDescriptor()->as_if<TCPSourceDescriptor>();
            if (descriptor)
            {
                if (sourceNumber == queryPlanSourceTcpCounter)
                {
                    /// Set socket port and serialize again.
                    descriptor->getSourceConfig()->setSocketPort(mockTcpServerPort);
                    auto serializedOperator = OperatorSerializationUtil::serializeOperator(deserializedSourceOperator);

                    /// Reconfigure the original operator id, because deserialization/serialization changes them.
                    serializedOperator.set_operatorid(value.operatorid());
                    swap(value, serializedOperator);
                    break;
                }
                ++queryPlanSourceTcpCounter;
            }
        }
    }
}
}
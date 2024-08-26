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
#include <Operators/Serialization/OperatorSerializationUtil.hpp>
#include <Operators/Serialization/SchemaSerializationUtil.hpp>
#include <fmt/core.h>
#include <gtest/gtest.h>

#include <GrpcService.hpp>
#include <IntegrationTestUtil.hpp>
#include <SingleNodeWorkerRPCService.pb.h>

namespace NES::IntegrationTestUtil
{
SchemaPtr loadSinkSchema(SerializableDecomposedQueryPlan& queryPlan)
{
    EXPECT_EQ(queryPlan.mutable_rootoperatorids()->size(), 1) << "Redirection is only implemented for Single Sink Queries";
    const auto rootOperatorId = queryPlan.mutable_rootoperatorids()->at(0);
    auto& rootOperator = queryPlan.mutable_operatormap()->at(rootOperatorId);
    EXPECT_TRUE(rootOperator.details().Is<SerializableOperator_SinkDetails>())
        << "Redirection expects the single root operator to be a sink operator";
    return SchemaSerializationUtil::deserializeSchema(rootOperator.outputschema());
}

QueryId registerQueryPlan(const SerializableDecomposedQueryPlan& queryPlan, GRPCServer& uut)
{
    grpc::ServerContext context;
    RegisterQueryRequest request;
    RegisterQueryReply reply;
    *request.mutable_decomposedqueryplan() = queryPlan;
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

void copyInputFile(const std::string_view inputFileName, const std::string_view querySpecificDataFileName)
{
    try
    {
        const auto from = fmt::format("{}/{}/{}", TEST_DATA_DIR, INPUT_CSV_FILES, inputFileName);
        const auto to = fmt::format("./{}", querySpecificDataFileName);
        copy_file(from, to, std::filesystem::copy_options::overwrite_existing);
    }
    catch (const std::filesystem::filesystem_error& e)
    {
        NES_ERROR("Error copying file: {}", e.what());
    }
}

void removeFile(const std::string_view filepath)
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

bool loadFile(
    SerializableDecomposedQueryPlan& queryPlan,
    const std::string_view queryFileName,
    const std::string_view dataFileName,
    const std::string_view querySpecificDataFileName)
{
    std::ifstream file(std::filesystem::path(TEST_DATA_DIR) / SERRIALIZED_QUERIES_DIRECTORY / (queryFileName));
    if (!file)
    {
        NES_ERROR("Query file is not available: {}/{}/{}", TEST_DATA_DIR, INPUT_CSV_FILES, queryFileName);
        return false;
    }
    if (!queryPlan.ParseFromIstream(&file))
    {
        NES_ERROR("Could not load protobuffer file: {}/{}/{}", TEST_DATA_DIR, INPUT_CSV_FILES, queryFileName);
        return false;
    }
    copyInputFile(dataFileName, querySpecificDataFileName);
    return true;
}

bool loadFile(SerializableDecomposedQueryPlan& queryPlan, const std::string_view queryFileName)
{
    std::ifstream f(std::filesystem::path(TEST_DATA_DIR) / SERRIALIZED_QUERIES_DIRECTORY / (queryFileName));
    if (!f)
    {
        NES_ERROR("Query file is not available: {}/{}/{}", TEST_DATA_DIR, INPUT_CSV_FILES, queryFileName);
        return false;
    }
    if (!queryPlan.ParseFromIstream(&f))
    {
        NES_ERROR("Could not load protobuffer file: {}/{}/{}", TEST_DATA_DIR, INPUT_CSV_FILES, queryFileName);
        return false;
    }
    return true;
}

void replaceFileSinkPath(SerializableDecomposedQueryPlan& decomposedQueryPlan, const std::string& fileName)
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

void replaceInputFileInCSVSources(SerializableDecomposedQueryPlan& decomposedQueryPlan, std::string newInputFileName)
{
    for (auto& pair : *decomposedQueryPlan.mutable_operatormap())
    {
        google::protobuf::uint64 key = pair.first;
        auto& value = pair.second; /// Note: non-const reference
        if (value.details().Is<SerializableOperator_SourceDetails>())
        {
            auto deserializedSourceOperator = OperatorSerializationUtil::deserializeOperator(value);
            auto sourceDescriptor = deserializedSourceOperator->as<SourceLogicalOperator>()->getSourceDescriptor();
            if (sourceDescriptor->getSourceName() == Sources::SourceDescriptor::PLUGIN_NAME_CSV)
            {
                /// Set socket port and serialize again.
                sourceDescriptor->setConfigType("filepath", newInputFileName);
                deserializedSourceOperator->as<SourceLogicalOperator>()->setSourceDescriptor(std::move(sourceDescriptor));
                auto serializedOperator = OperatorSerializationUtil::serializeOperator(deserializedSourceOperator);

                /// Reconfigure the original operator id, because deserialization/serialization changes them.
                serializedOperator.set_operatorid(value.operatorid());
                swap(value, serializedOperator);
            }
        }
    }
}

void replacePortInTcpSources(SerializableDecomposedQueryPlan& decomposedQueryPlan, const uint32_t mockTcpServerPort, const int sourceNumber)
{
    int queryPlanSourceTcpCounter = 0;
    for (auto& pair : *decomposedQueryPlan.mutable_operatormap())
    {
        google::protobuf::uint64 key = pair.first;
        auto& value = pair.second; /// Note: non-const reference
        if (value.details().Is<SerializableOperator_SourceDetails>())
        {
            auto deserializedSourceOperator = OperatorSerializationUtil::deserializeOperator(value);
            auto sourceDescriptor = deserializedSourceOperator->as<SourceLogicalOperator>()->getSourceDescriptor();
            if (sourceDescriptor->getSourceName() == Sources::SourceDescriptor::PLUGIN_NAME_TCP)
            {
                if (sourceNumber == queryPlanSourceTcpCounter)
                {
                    /// Set socket port and serialize again.
                    sourceDescriptor->setConfigType("socket_port", mockTcpServerPort);
                    deserializedSourceOperator->as<SourceLogicalOperator>()->setSourceDescriptor(std::move(sourceDescriptor));
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
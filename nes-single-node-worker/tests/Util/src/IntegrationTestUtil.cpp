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

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <thread>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Serialization/OperatorSerializationUtil.hpp>
#include <Operators/Serialization/SchemaSerializationUtil.hpp>
#include <Sinks/FileSink.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Common.hpp>
#include <Util/Strings.hpp>
#include <fmt/core.h>
#include <grpcpp/support/status.h>
#include <gtest/gtest.h>
#include <ErrorHandling.hpp>
#include <GrpcService.hpp>
#include <IntegrationTestUtil.hpp>
#include <SerializableDecomposedQueryPlan.pb.h>
#include <SingleNodeWorkerRPCService.pb.h>

namespace NES::IntegrationTestUtil
{

[[maybe_unused]] std::vector<Memory::TupleBuffer> createBuffersFromCSVFile(
    const std::string& csvFile,
    Schema schema,
    Memory::AbstractBufferProvider& bufferProvider,
    uint64_t originId,
    const std::string& timestampFieldName,
    bool skipFirstLine)
{
    std::vector<Memory::TupleBuffer> recordBuffers;
    INVARIANT(std::filesystem::exists(std::filesystem::path(csvFile)), "CSVFile {} does not exist", csvFile);

    /// Creating everything for the csv parser
    std::ifstream file(csvFile);

    if (skipFirstLine)
    {
        std::string line;
        std::getline(file, line);
    }

    auto getPhysicalTypes = [](Schema schema)
    {
        std::vector<DataType> retVector;
        for (const auto& field : schema.getFields())
        {
            retVector.push_back(field.dataType);
        }

        return retVector;
    };

    const auto maxTuplesPerBuffer = bufferProvider.getBufferSize() / schema.getSizeOfSchemaInBytes();
    auto tupleCount = 0UL;
    auto tupleBuffer = bufferProvider.getBufferBlocking();
    const auto numberOfSchemaFields = schema.getNumberOfFields();
    const auto physicalTypes = getPhysicalTypes(schema);

    uint64_t sequenceNumber = 0;
    uint64_t watermarkTS = 0;
    for (std::string line; std::getline(file, line);)
    {
        auto testBuffer = Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(tupleBuffer, schema);
        auto values = Util::splitWithStringDelimiter<std::string>(line, ",");

        /// iterate over fields of schema and cast string values to correct type
        for (uint64_t j = 0; j < numberOfSchemaFields; j++)
        {
            writeFieldValueToTupleBuffer(values[j], j, testBuffer, schema, tupleCount, bufferProvider);
        }
        if (schema.contains(timestampFieldName))
        {
            watermarkTS = std::max(watermarkTS, testBuffer[tupleCount][timestampFieldName].read<uint64_t>());
        }
        ++tupleCount;

        if (tupleCount >= maxTuplesPerBuffer)
        {
            tupleBuffer.setNumberOfTuples(tupleCount);
            tupleBuffer.setOriginId(OriginId(originId));
            tupleBuffer.setSequenceNumber(SequenceNumber(++sequenceNumber));
            tupleBuffer.setWatermark(Runtime::Timestamp(watermarkTS));
            NES_DEBUG("watermarkTS {} sequenceNumber {} originId {}", watermarkTS, sequenceNumber, originId);

            recordBuffers.emplace_back(tupleBuffer);
            tupleBuffer = bufferProvider.getBufferBlocking();
            tupleCount = 0UL;
            watermarkTS = 0UL;
        }
    }

    if (tupleCount > 0)
    {
        tupleBuffer.setNumberOfTuples(tupleCount);
        tupleBuffer.setOriginId(OriginId(originId));
        tupleBuffer.setSequenceNumber(SequenceNumber(++sequenceNumber));
        tupleBuffer.setWatermark(Runtime::Timestamp(watermarkTS));
        recordBuffers.emplace_back(tupleBuffer);
        NES_DEBUG("watermarkTS {} sequenceNumber {} originId {}", watermarkTS, sequenceNumber, originId);
    }

    return recordBuffers;
}

void writeFieldValueToTupleBuffer(
    std::string inputString,
    uint64_t schemaFieldIndex,
    Memory::MemoryLayouts::TestTupleBuffer& tupleBuffer,
    Schema schema,
    uint64_t tupleCount,
    Memory::AbstractBufferProvider& bufferProvider)
{
    const auto dataType = schema.getFieldAt(schemaFieldIndex).dataType;

    PRECONDITION(not inputString.empty(), "Input string for parsing is empty");
    if (dataType.isNumeric())
    {
        DataTypeUtil::dispatchByNumericalOrBoolOrCharType(
            dataType.type,
            [&tupleBuffer, tupleCount, schemaFieldIndex, input = std::move(inputString)]<typename T>()
            {
                const auto parsedValue = Util::from_chars<T>(input);
                INVARIANT(parsedValue.has_value(), "Failed to parse input string {} to type {}", input, typeid(T).name());
                tupleBuffer[tupleCount][schemaFieldIndex].write<T>(parsedValue.value());
            });
        return;
    }
    if (dataType.isVarSized())
    {
        NES_TRACE(
            "Parser::writeFieldValueToTupleBuffer(): trying to write the variable length input string: {}"
            "to tuple buffer",
            inputString);
        tupleBuffer[tupleCount].writeVarSized(schemaFieldIndex, inputString, bufferProvider);
        return;
    }
    throw UnknownPhysicalType("Not supporting type: {}", magic_enum::enum_name(dataType.type));
}

Schema loadSinkSchema(SerializableDecomposedQueryPlan& queryPlan)
{
    EXPECT_EQ(queryPlan.mutable_rootoperatorids()->size(), 1) << "Redirection is only implemented for Single Sink Queries";
    const auto rootOperatorId = queryPlan.mutable_rootoperatorids()->at(0);
    auto& rootOperator = queryPlan.mutable_operatormap()->at(rootOperatorId);
    EXPECT_TRUE(rootOperator.details().Is<SerializableOperator_SinkLogicalOperator>())
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

bool isQueryStopped(QueryId queryId, GRPCServer& uut)
{
    grpc::ServerContext context;
    QuerySummaryRequest request;
    QuerySummaryReply reply;
    request.set_queryid(queryId.getRawValue());
    EXPECT_TRUE(uut.RequestQuerySummary(&context, &request, &reply).ok());
    return reply.status() == Stopped;
}

void stopQuery(QueryId queryId, StopQueryRequest::QueryTerminationType type, GRPCServer& uut)
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

QuerySummaryReply querySummary(QueryId queryId, GRPCServer& uut)
{
    grpc::ServerContext context;
    QuerySummaryRequest request;
    QuerySummaryReply reply;
    request.set_queryid(queryId.getRawValue());
    EXPECT_TRUE(uut.RequestQuerySummary(&context, &request, &reply).ok());
    return reply;
}

void querySummaryFailure(QueryId queryId, GRPCServer& uut, grpc::StatusCode statusCode)
{
    grpc::ServerContext context;
    QuerySummaryRequest request;
    QuerySummaryReply reply;
    request.set_queryid(queryId.getRawValue());
    auto response = uut.RequestQuerySummary(&context, &request, &reply);
    EXPECT_FALSE(response.ok()) << "Expected QuerySummary request to fail";
    EXPECT_EQ(response.error_code(), statusCode);
}

QueryStatus queryStatus(QueryId queryId, GRPCServer& uut)
{
    grpc::ServerContext context;
    QuerySummaryRequest request;
    QuerySummaryReply reply;
    request.set_queryid(queryId.getRawValue());
    EXPECT_TRUE(uut.RequestQuerySummary(&context, &request, &reply).ok());
    return reply.status();
}

testing::AssertionResult waitForQueryToEnd(QueryId queryId, GRPCServer& uut)
{
    /// We are waiting for 2 seconds (25ms * 40 = 1s)
    constexpr size_t maxNumberOfTimeoutChecks = 80;
    size_t numTimeouts = 0;
    auto currentQueryStatus = queryStatus(queryId, uut);
    while (currentQueryStatus != Stopped && currentQueryStatus != Failed)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
        if (++numTimeouts > maxNumberOfTimeoutChecks)
        {
            return testing::AssertionFailure(testing::Message("Hit timeout while waiting for expected query status or failure."));
        }
        currentQueryStatus = queryStatus(queryId, uut);
    }
    return testing::AssertionSuccess();
}

std::vector<std::pair<Runtime::Execution::QueryStatus, std::chrono::system_clock::time_point>> queryLog(QueryId queryId, GRPCServer& uut)
{
    grpc::ServerContext context;
    QueryLogRequest request;
    QueryLogReply reply;
    request.set_queryid(queryId.getRawValue());
    EXPECT_TRUE(uut.RequestQueryLog(&context, &request, &reply).ok());
    std::vector<std::pair<Runtime::Execution::QueryStatus, std::chrono::system_clock::time_point>> logs;
    for (const auto& log : reply.entries())
    {
        auto time_point = std::chrono::system_clock::time_point(std::chrono::milliseconds(log.unixtimeinms()));
        auto status = magic_enum::enum_cast<Runtime::Execution::QueryStatus>(log.status());
        if (status.has_value())
        {
            logs.emplace_back(status.value(), time_point);
        }
    }
    return logs;
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
        NES_ERROR("Query file is not available: {}/{}/{}", TEST_DATA_DIR, SERRIALIZED_QUERIES_DIRECTORY, queryFileName);
        return false;
    }
    if (!queryPlan.ParseFromIstream(&file))
    {
        NES_ERROR("Could not load protobuffer file: {}/{}/{}", TEST_DATA_DIR, SERRIALIZED_QUERIES_DIRECTORY, queryFileName);
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
        NES_ERROR("Query file is not available: {}/{}/{}", TEST_DATA_DIR, SERRIALIZED_QUERIES_DIRECTORY, queryFileName);
        return false;
    }
    if (!queryPlan.ParseFromIstream(&f))
    {
        NES_ERROR("Could not load protobuffer file: {}/{}/{}", TEST_DATA_DIR, SERRIALIZED_QUERIES_DIRECTORY, queryFileName);
        return false;
    }
    return true;
}

void replaceFileSinkPath(SerializableDecomposedQueryPlan& decomposedQueryPlan, const std::string& filePathNew)
{
    EXPECT_EQ(decomposedQueryPlan.mutable_rootoperatorids()->size(), 1) << "Redirection is only implemented for Single Sink Queries";
    const auto rootOperatorId = decomposedQueryPlan.mutable_rootoperatorids()->at(0);
    auto& rootOperator = decomposedQueryPlan.mutable_operatormap()->at(rootOperatorId);

    EXPECT_TRUE(rootOperator.details().Is<SerializableOperator_SinkLogicalOperator>())
        << "Redirection expects the single root operator to be a sink operator";
    const auto deserializedSinkOperator = NES::Util::as<SinkLogicalOperator>(OperatorSerializationUtil::deserializeOperator(rootOperator));
    auto descriptor = NES::Util::as<SinkLogicalOperator>(deserializedSinkOperator)->getSinkDescriptorRef();
    if (descriptor.sinkType == Sinks::FileSink::NAME)
    {
        const auto deserializedOutputSchema = SchemaSerializationUtil::deserializeSchema(rootOperator.outputschema());
        auto configCopy = descriptor.config;
        configCopy.at(Sinks::ConfigParametersFile::FILEPATH) = filePathNew;
        auto sinkDescriptorUpdated
            = std::make_unique<Sinks::SinkDescriptor>(descriptor.sinkType, std::move(configCopy), descriptor.addTimestamp);
        sinkDescriptorUpdated->schema = deserializedOutputSchema;
        auto sinkLogicalOperatorUpdated
            = std::make_shared<SinkLogicalOperator>(deserializedSinkOperator->sinkName, deserializedSinkOperator->getId());
        sinkLogicalOperatorUpdated->sinkDescriptor = std::move(sinkDescriptorUpdated);
        sinkLogicalOperatorUpdated->setOutputSchema(deserializedOutputSchema);
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(sinkLogicalOperatorUpdated);

        /// Reconfigure the original operator id, and childrenIds because deserialization/serialization changes them.
        serializedOperator.set_operatorid(rootOperator.operatorid());
        *serializedOperator.mutable_childrenids() = rootOperator.childrenids();

        swap(rootOperator, serializedOperator);
    }
}

void replaceInputFileInFileSources(SerializableDecomposedQueryPlan& decomposedQueryPlan, std::string newInputFileName)
{
    for (auto& pair : *decomposedQueryPlan.mutable_operatormap())
    {
        auto& value = pair.second; /// Note: non-const reference
        if (value.details().Is<SerializableOperator_SourceDescriptorLogicalOperator>())
        {
            auto deserializedSourceOperator = OperatorSerializationUtil::deserializeOperator(value);
            const auto sourceDescriptor
                = NES::Util::as<SourceDescriptorLogicalOperator>(deserializedSourceOperator)->getSourceDescriptorRef();
            if (sourceDescriptor.sourceType == "File")
            {
                /// We violate the immutability constrain of the SourceDescriptor here to patch in the correct file path.
                Configurations::DescriptorConfig::Config configUpdated = sourceDescriptor.config;
                configUpdated.at("filePath") = newInputFileName;
                auto sourceDescriptorUpdated = std::make_unique<Sources::SourceDescriptor>(
                    sourceDescriptor.schema,
                    sourceDescriptor.logicalSourceName,
                    sourceDescriptor.sourceType,
                    sourceDescriptor.parserConfig,
                    std::move(configUpdated));

                const auto sourceDescriptorLogicalOperatorUpdated = std::make_shared<SourceDescriptorLogicalOperator>(
                    std::move(sourceDescriptorUpdated),
                    deserializedSourceOperator->getId(),
                    NES::Util::as<SourceDescriptorLogicalOperator>(deserializedSourceOperator)->getOriginId());
                auto serializedOperator = OperatorSerializationUtil::serializeOperator(sourceDescriptorLogicalOperatorUpdated);

                /// Reconfigure the original operator id, because deserialization/serialization changes them.
                serializedOperator.set_operatorid(value.operatorid());
                swap(value, serializedOperator);
            }
        }
    }
}

void replacePortInTCPSources(SerializableDecomposedQueryPlan& decomposedQueryPlan, const uint16_t mockTcpServerPort, const int sourceNumber)
{
    int queryPlanTCPSourceCounter = 0;
    for (auto& pair : *decomposedQueryPlan.mutable_operatormap())
    {
        auto& value = pair.second; /// Note: non-const reference
        if (value.details().Is<SerializableOperator_SourceDescriptorLogicalOperator>())
        {
            auto deserializedSourceOperator = OperatorSerializationUtil::deserializeOperator(value);
            const auto sourceDescriptor
                = NES::Util::as<SourceDescriptorLogicalOperator>(deserializedSourceOperator)->getSourceDescriptorRef();
            if (sourceDescriptor.sourceType == "TCP")
            {
                if (sourceNumber == queryPlanTCPSourceCounter)
                {
                    /// We violate the immutability constrain of the SourceDescriptor here to patch in the correct port.
                    Configurations::DescriptorConfig::Config configUpdated = sourceDescriptor.config;
                    configUpdated.at("socketPort") = static_cast<uint32_t>(mockTcpServerPort);
                    auto sourceDescriptorUpdated = std::make_unique<Sources::SourceDescriptor>(
                        sourceDescriptor.schema,
                        sourceDescriptor.logicalSourceName,
                        sourceDescriptor.sourceType,
                        sourceDescriptor.parserConfig,
                        std::move(configUpdated));

                    const auto sourceDescriptorLogicalOperatorUpdated = std::make_shared<SourceDescriptorLogicalOperator>(
                        std::move(sourceDescriptorUpdated),
                        deserializedSourceOperator->getId(),
                        NES::Util::as<SourceDescriptorLogicalOperator>(deserializedSourceOperator)->getOriginId());
                    auto serializedOperator = OperatorSerializationUtil::serializeOperator(sourceDescriptorLogicalOperatorUpdated);

                    /// Reconfigure the original operator id, because deserialization/serialization changes them.
                    serializedOperator.set_operatorid(value.operatorid());
                    swap(value, serializedOperator);
                    break;
                }
                ++queryPlanTCPSourceCounter;
            }
        }
    }
}

std::string getUniqueTestIdentifier()
{
    const auto timestamp
        = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count();
    const testing::TestInfo* const test_info = testing::UnitTest::GetInstance()->current_test_info();

    auto uniqueTestIdentifier
        = fmt::format("{}_{}_{}", std::string(test_info->test_suite_name()), std::string(test_info->name()), timestamp);
    std::ranges::replace(uniqueTestIdentifier, '/', '_');
    return uniqueTestIdentifier;
}
}

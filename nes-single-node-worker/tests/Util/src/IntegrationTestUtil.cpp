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
#include <utility>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Serialization/OperatorSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Sinks/FileSink.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Common.hpp>
#include <Util/Strings.hpp>
#include <fmt/core.h>
#include <grpcpp/support/status.h>
#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <GrpcService.hpp>
#include <IntegrationTestUtil.hpp>

#include <Configurations/Descriptor.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SerializableQueryPlan.pb.h>
#include <SingleNodeWorkerRPCService.pb.h>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Common/PhysicalTypes/VariableSizedDataPhysicalType.hpp>

namespace NES::IntegrationTestUtil
{

[[maybe_unused]] std::vector<Memory::TupleBuffer> createBuffersFromCSVFile(
    const std::string& csvFile,
    const Schema& schema,
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

    auto getPhysicalTypes = [](const Schema& schema)
    {
        std::vector<std::unique_ptr<PhysicalType>> retVector;
        DefaultPhysicalTypeFactory defaultPhysicalTypeFactory;
        for (const auto& field : schema)
        {
            retVector.push_back(defaultPhysicalTypeFactory.getPhysicalType(field.getDataType()));
        }

        return retVector;
    };

    const auto maxTuplesPerBuffer = bufferProvider.getBufferSize() / schema.getSchemaSizeInBytes();
    auto tupleCount = 0UL;
    auto tupleBuffer = bufferProvider.getBufferBlocking();
    const auto numberOfSchemaFields = schema.getFieldCount();
    const auto physicalTypes = getPhysicalTypes(schema);

    uint64_t sequenceNumber = 0;
    uint64_t watermarkTS = 0;
    for (std::string line; std::getline(file, line);)
    {
        auto testBuffer = Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(tupleBuffer, schema);
        auto values = NES::Util::splitWithStringDelimiter<std::string>(line, ",");

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
            tupleBuffer.setWatermark(Timestamp(watermarkTS));
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
        tupleBuffer.setWatermark(Timestamp(watermarkTS));
        recordBuffers.emplace_back(tupleBuffer);
        NES_DEBUG("watermarkTS {} sequenceNumber {} originId {}", watermarkTS, sequenceNumber, originId);
    }

    return recordBuffers;
}

void writeFieldValueToTupleBuffer(
    std::string inputString,
    uint64_t schemaFieldIndex,
    Memory::MemoryLayouts::TestTupleBuffer& tupleBuffer,
    const Schema& schema,
    uint64_t tupleCount,
    Memory::AbstractBufferProvider& bufferProvider)
{
    auto dataType = schema.getFieldByIndex(schemaFieldIndex).getDataType();
    auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);

    INVARIANT(!inputString.empty(), "Input string for parsing is empty");
    /// TODO #371 replace with csv parsing library
    try
    {
        if (auto* basicPhysicalType = dynamic_cast<BasicPhysicalType*>(physicalType.get()))
        {
            switch (basicPhysicalType->nativeType)
            {
                case NES::BasicPhysicalType::NativeType::INT_8: {
                    tupleBuffer[tupleCount][schemaFieldIndex].write<int8_t>(*NES::Util::from_chars<int8_t>(inputString));
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_16: {
                    tupleBuffer[tupleCount][schemaFieldIndex].write<int16_t>(*NES::Util::from_chars<int16_t>(inputString));
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_32: {
                    tupleBuffer[tupleCount][schemaFieldIndex].write<int32_t>(*NES::Util::from_chars<int32_t>(inputString));
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_64: {
                    tupleBuffer[tupleCount][schemaFieldIndex].write<int64_t>(*NES::Util::from_chars<int64_t>(inputString));
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_8: {
                    tupleBuffer[tupleCount][schemaFieldIndex].write<uint8_t>(*NES::Util::from_chars<uint8_t>(inputString));
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_16: {
                    tupleBuffer[tupleCount][schemaFieldIndex].write<uint16_t>(*NES::Util::from_chars<uint16_t>(inputString));
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_32: {
                    tupleBuffer[tupleCount][schemaFieldIndex].write<uint32_t>(*NES::Util::from_chars<uint32_t>(inputString));
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_64: {
                    tupleBuffer[tupleCount][schemaFieldIndex].write<uint64_t>(*NES::Util::from_chars<uint64_t>(inputString));
                    break;
                }
                case NES::BasicPhysicalType::NativeType::FLOAT: {
                    auto optValue = NES::Util::from_chars<float>(NES::Util::replaceAll(inputString, ",", "."));
                    tupleBuffer[tupleCount][schemaFieldIndex].write<float>(*optValue);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::DOUBLE: {
                    auto optValue = NES::Util::from_chars<double>(NES::Util::replaceAll(inputString, ",", "."));
                    tupleBuffer[tupleCount][schemaFieldIndex].write<double>(*optValue);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::CHAR: {
                    ///verify that only a single char was transmitted
                    if (inputString.size() > 1)
                    {
                        NES_FATAL_ERROR(
                            "SourceFormatIterator::mqttMessageToNESBuffer: Received non char Value for CHAR Field {}", inputString.c_str());
                        throw std::invalid_argument("Value " + inputString + " is not a char");
                    }
                    char value = inputString.at(0);
                    tupleBuffer[tupleCount][schemaFieldIndex].write<char>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::BOOLEAN: {
                    tupleBuffer[tupleCount][schemaFieldIndex].write<bool>(*NES::Util::from_chars<bool>(inputString));
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UNDEFINED:
                    NES_FATAL_ERROR("Parser::writeFieldValueToTupleBuffer: Field Type UNDEFINED");
            }
        }
        else if (dynamic_cast<VariableSizedDataPhysicalType*>(physicalType.get()) != nullptr)
        {
            NES_TRACE(
                "Parser::writeFieldValueToTupleBuffer(): trying to write the variable length input string: {}"
                "to tuple buffer",
                inputString);
            tupleBuffer[tupleCount].writeVarSized(schemaFieldIndex, inputString, bufferProvider);
        }
        else
        {
            throw NotImplemented();
        }
    }
    catch (const std::exception& e)
    {
        NES_ERROR("Failed to convert inputString to desired NES data type. Error: {}", e.what());
    }
}

Schema loadSinkSchema(SerializableQueryPlan& queryPlan)
{
    EXPECT_EQ(queryPlan.mutable_rootoperatorids()->size(), 1) << "Redirection is only implemented for Single Sink Queries";
    const auto rootoperatorId = queryPlan.mutable_rootoperatorids()->at(0);
    auto& rootOperator = queryPlan.mutable_operatormap()->at(rootoperatorId);
    EXPECT_TRUE(rootOperator.has_sink()) << "Redirection expects the single root operator to be a sink operator";
    return SchemaSerializationUtil::deserializeSchema(rootOperator.sink().sinkdescriptor().sinkschema());
}

QueryId registerQueryPlan(const SerializableQueryPlan& queryPlan, GRPCServer& uut)
{
    grpc::ServerContext context;
    RegisterQueryRequest request;
    RegisterQueryReply reply;
    *request.mutable_queryplan() = queryPlan;
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

::QueryStatus queryStatus(QueryId queryId, GRPCServer& uut)
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
    while (currentQueryStatus != ::QueryStatus::Stopped && currentQueryStatus != ::QueryStatus::Failed)
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

std::vector<std::pair<QueryStatus, std::chrono::system_clock::time_point>> queryLog(QueryId queryId, GRPCServer& uut)
{
    grpc::ServerContext context;
    QueryLogRequest request;
    QueryLogReply reply;
    request.set_queryid(queryId.getRawValue());
    EXPECT_TRUE(uut.RequestQueryLog(&context, &request, &reply).ok());
    std::vector<std::pair<QueryStatus, std::chrono::system_clock::time_point>> logs;
    for (const auto& log : reply.entries())
    {
        auto time_point = std::chrono::system_clock::time_point(std::chrono::milliseconds(log.unixtimeinms()));
        auto status = magic_enum::enum_cast<QueryStatus>(log.status());
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
    SerializableQueryPlan& queryPlan,
    const std::string_view queryFileName,
    const std::string_view dataFileName,
    const std::string_view querySpecificDataFileName)
{
    std::ifstream file(std::filesystem::path(SERIALIZED_QUERIES_DIR) / (queryFileName));
    if (!file)
    {
        NES_ERROR("Query file is not available: {}/{}", SERIALIZED_QUERIES_DIR, queryFileName);
        return false;
    }
    if (!queryPlan.ParseFromIstream(&file))
    {
        NES_ERROR("Could not load protobuffer file: {}/{}", SERIALIZED_QUERIES_DIR, queryFileName);
        return false;
    }
    copyInputFile(dataFileName, querySpecificDataFileName);
    return true;
}

bool loadFile(SerializableQueryPlan& queryPlan, const std::string_view queryFileName)
{
    std::ifstream f(std::filesystem::path(SERIALIZED_QUERIES_DIR) / (queryFileName));
    if (!f)
    {
        NES_ERROR("Query file is not available: {}/{}", SERIALIZED_QUERIES_DIR, queryFileName);
        return false;
    }
    if (!queryPlan.ParseFromIstream(&f))
    {
        NES_ERROR("Could not load protobuffer file: {}/{}", SERIALIZED_QUERIES_DIR, queryFileName);
        return false;
    }
    return true;
}

void replaceFileSinkPath(SerializableQueryPlan& decomposedQueryPlan, const std::string& filePathNew)
{
    EXPECT_EQ(decomposedQueryPlan.mutable_rootoperatorids()->size(), 1) << "Redirection is only implemented for Single Sink Queries";
    const auto rootoperatorId = decomposedQueryPlan.mutable_rootoperatorids()->at(0);
    auto& rootOperator = decomposedQueryPlan.mutable_operatormap()->at(rootoperatorId);

    EXPECT_TRUE(rootOperator.has_sink()) << "Redirection expects the single root operator to be a sink operator";
    const auto deserializedSinkOperator = OperatorSerializationUtil::deserializeOperator(rootOperator).get<SinkLogicalOperator>();
    auto descriptor = deserializedSinkOperator.sinkDescriptor;
    if (descriptor->sinkType == Sinks::FileSink::NAME)
    {
        const auto deserializedOutputSchema = SchemaSerializationUtil::deserializeSchema(rootOperator.sink().sinkdescriptor().sinkschema());
        auto configCopy = descriptor->config;
        configCopy.at(Sinks::ConfigParametersFile::FILEPATH) = filePathNew;
        auto sinkDescriptorUpdated
            = std::make_unique<Sinks::SinkDescriptor>(descriptor->sinkType, std::move(configCopy), descriptor->addTimestamp);
        sinkDescriptorUpdated->schema = deserializedOutputSchema;
        auto sinkLogicalOperatorUpdated = SinkLogicalOperator(deserializedSinkOperator.sinkName);
        sinkLogicalOperatorUpdated.sinkDescriptor = std::move(sinkDescriptorUpdated);
        sinkLogicalOperatorUpdated.setOutputSchema(deserializedOutputSchema);
        auto serializedOperator = sinkLogicalOperatorUpdated.serialize();

        /// Reconfigure the original operator id, and childrenIds because deserialization/serialization changes them.
        serializedOperator.set_operator_id(rootOperator.operator_id());
        *serializedOperator.mutable_children_ids() = rootOperator.children_ids();

        swap(rootOperator, serializedOperator);
    }
}

void replaceInputFileInFileSources(SerializableQueryPlan& decomposedQueryPlan, const std::string& newInputFileName)
{
    for (auto& pair : *decomposedQueryPlan.mutable_operatormap())
    {
        auto& value = pair.second; /// Note: non-const reference
        if (value.has_source())
        {
            auto deserializedSourceOperator = OperatorSerializationUtil::deserializeOperator(value);
            const auto sourceDescriptor = deserializedSourceOperator.get<SourceDescriptorLogicalOperator>().getSourceDescriptor();
            if (sourceDescriptor->sourceType == "File")
            {
                /// We violate the immutability constrain of the SourceDescriptor here to patch in the correct file path.
                NES::Configurations::DescriptorConfig::Config configUpdated = sourceDescriptor->config;
                configUpdated.at("filePath") = newInputFileName;
                auto sourceDescriptorUpdated = std::make_unique<Sources::SourceDescriptor>(
                    sourceDescriptor->schema,
                    sourceDescriptor->logicalSourceName,
                    sourceDescriptor->sourceType,
                    sourceDescriptor->numberOfBuffersInSourceLocalBufferPool,
                    sourceDescriptor->parserConfig,
                    std::move(configUpdated));

                auto sourceDescriptorLogicalOperatorUpdated = SourceDescriptorLogicalOperator(std::move(sourceDescriptorUpdated))
                                                                  .withOutputOriginIds(deserializedSourceOperator.getOutputOriginIds());
                auto serializedOperator = sourceDescriptorLogicalOperatorUpdated.serialize();

                /// Reconfigure the original operator id, because deserialization/serialization changes them.
                serializedOperator.set_operator_id(value.operator_id());
                swap(value, serializedOperator);
            }
        }
    }
}

void replacePortInTCPSources(SerializableQueryPlan& decomposedQueryPlan, const uint16_t mockTcpServerPort, const int sourceNumber)
{
    int queryPlanTCPSourceCounter = 0;
    for (auto& pair : *decomposedQueryPlan.mutable_operatormap())
    {
        auto& value = pair.second; /// Note: non-const reference
        if (value.has_source())
        {
            auto deserializedSourceOperator = OperatorSerializationUtil::deserializeOperator(value);
            const auto sourceDescriptor = deserializedSourceOperator.get<SourceDescriptorLogicalOperator>().getSourceDescriptor();
            if (sourceDescriptor->sourceType == "TCP")
            {
                if (sourceNumber == queryPlanTCPSourceCounter)
                {
                    /// We violate the immutability constrain of the SourceDescriptor here to patch in the correct port.
                    NES::Configurations::DescriptorConfig::Config configUpdated = sourceDescriptor->config;
                    configUpdated.at("socketPort") = static_cast<uint32_t>(mockTcpServerPort);
                    auto sourceDescriptorUpdated = std::make_unique<Sources::SourceDescriptor>(
                        sourceDescriptor->schema,
                        sourceDescriptor->logicalSourceName,
                        sourceDescriptor->sourceType,
                        sourceDescriptor->numberOfBuffersInSourceLocalBufferPool,
                        sourceDescriptor->parserConfig,
                        std::move(configUpdated));

                    auto sourceDescriptorLogicalOperatorUpdated = SourceDescriptorLogicalOperator(std::move(sourceDescriptorUpdated));
                    auto serializedOperator
                        = sourceDescriptorLogicalOperatorUpdated.withOutputOriginIds(deserializedSourceOperator.getOutputOriginIds())
                              .serialize();

                    /// Reconfigure the original operator id, because deserialization/serialization changes them.
                    serializedOperator.set_operator_id(value.operator_id());
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

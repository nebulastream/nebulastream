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
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/Serialization/OperatorSerializationUtil.hpp>
#include <Operators/Serialization/SchemaSerializationUtil.hpp>
#include <Sources/SourceCSV.hpp>
#include <Sources/SourceTCP.hpp>
#include <Util/Common.hpp>
#include <fmt/core.h>
#include <gtest/gtest.h>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

#include <Util/Common.hpp>
#include <GrpcService.hpp>
#include <IntegrationTestUtil.hpp>
#include <SingleNodeWorkerRPCService.pb.h>

namespace NES::IntegrationTestUtil
{

[[maybe_unused]] std::vector<Memory::TupleBuffer> createBuffersFromCSVFile(
    const std::string& csvFile,
    const SchemaPtr& schema,
    Memory::AbstractBufferProvider& bufferProvider,
    uint64_t originId,
    const std::string& timestampFieldName,
    bool skipFirstLine)
{
    std::vector<Memory::TupleBuffer> recordBuffers;
    NES_ASSERT2_FMT(std::filesystem::exists(std::filesystem::path(csvFile)), "CSVFile " << csvFile << " does not exist!!!");

    /// Creating everything for the csv parser
    std::ifstream file(csvFile);

    if (skipFirstLine)
    {
        std::string line;
        std::getline(file, line);
    }

    auto getPhysicalTypes = [](const SchemaPtr& schema)
    {
        std::vector<PhysicalTypePtr> retVector;
        DefaultPhysicalTypeFactory defaultPhysicalTypeFactory;
        for (const auto& field : schema->fields)
        {
            auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
            retVector.push_back(physicalField);
        }

        return retVector;
    };

    const auto maxTuplesPerBuffer = bufferProvider.getBufferSize() / schema->getSchemaSizeInBytes();
    auto tupleCount = 0UL;
    auto tupleBuffer = bufferProvider.getBufferBlocking();
    const auto numberOfSchemaFields = schema->fields.size();
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
        if (schema->contains(timestampFieldName))
        {
            watermarkTS = std::max(watermarkTS, testBuffer[tupleCount][timestampFieldName].read<uint64_t>());
        }
        ++tupleCount;

        if (tupleCount >= maxTuplesPerBuffer)
        {
            tupleBuffer.setNumberOfTuples(tupleCount);
            tupleBuffer.setOriginId(OriginId(originId));
            tupleBuffer.setSequenceNumber(++sequenceNumber);
            tupleBuffer.setWatermark(watermarkTS);
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
        tupleBuffer.setSequenceNumber(++sequenceNumber);
        tupleBuffer.setWatermark(watermarkTS);
        recordBuffers.emplace_back(tupleBuffer);
        NES_DEBUG("watermarkTS {} sequenceNumber {} originId {}", watermarkTS, sequenceNumber, originId);
    }

    return recordBuffers;
}

void writeFieldValueToTupleBuffer(
    std::string inputString,
    uint64_t schemaFieldIndex,
    Memory::MemoryLayouts::TestTupleBuffer& tupleBuffer,
    const SchemaPtr& schema,
    uint64_t tupleCount,
    Memory::AbstractBufferProvider& bufferProvider)
{
    auto fields = schema->fields;
    auto dataType = fields[schemaFieldIndex]->getDataType();
    auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);

    if (inputString.empty())
    {
        throw Exceptions::RuntimeException("Input string for parsing is empty");
    }
    /// TODO #371 replace with csv parsing library
    try
    {
        if (physicalType->isBasicType())
        {
            auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
            switch (basicPhysicalType->nativeType)
            {
                case NES::BasicPhysicalType::NativeType::INT_8: {
                    auto value = static_cast<int8_t>(std::stoi(inputString));
                    tupleBuffer[tupleCount][schemaFieldIndex].write<int8_t>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_16: {
                    auto value = static_cast<int16_t>(std::stol(inputString));
                    tupleBuffer[tupleCount][schemaFieldIndex].write<int16_t>(value);

                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_32: {
                    auto value = static_cast<int32_t>(std::stol(inputString));
                    tupleBuffer[tupleCount][schemaFieldIndex].write<int32_t>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_64: {
                    auto value = static_cast<int64_t>(std::stoll(inputString));
                    tupleBuffer[tupleCount][schemaFieldIndex].write<int64_t>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_8: {
                    auto value = static_cast<uint8_t>(std::stoi(inputString));
                    tupleBuffer[tupleCount][schemaFieldIndex].write<uint8_t>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_16: {
                    auto value = static_cast<uint16_t>(std::stoul(inputString));
                    tupleBuffer[tupleCount][schemaFieldIndex].write<uint16_t>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_32: {
                    auto value = static_cast<uint32_t>(std::stoul(inputString));
                    tupleBuffer[tupleCount][schemaFieldIndex].write<uint32_t>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_64: {
                    auto value = static_cast<uint64_t>(std::stoull(inputString));
                    tupleBuffer[tupleCount][schemaFieldIndex].write<uint64_t>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::FLOAT: {
                    Util::findAndReplaceAll(inputString, ",", ".");
                    auto value = static_cast<float>(std::stof(inputString));
                    tupleBuffer[tupleCount][schemaFieldIndex].write<float>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::DOUBLE: {
                    auto value = static_cast<double>(std::stod(inputString));
                    tupleBuffer[tupleCount][schemaFieldIndex].write<double>(value);
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
                    ///verify that a valid bool was transmitted (valid{true,false,0,1})
                    bool value = !strcasecmp(inputString.c_str(), "true") || !strcasecmp(inputString.c_str(), "1");
                    if (!value)
                    {
                        if (strcasecmp(inputString.c_str(), "false") && strcasecmp(inputString.c_str(), "0"))
                        {
                            NES_FATAL_ERROR(
                                "Parser::writeFieldValueToTupleBuffer: Received non boolean value for BOOLEAN field: {}",
                                inputString.c_str());
                            throw std::invalid_argument("Value " + inputString + " is not a boolean");
                        }
                    }
                    tupleBuffer[tupleCount][schemaFieldIndex].write<bool>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UNDEFINED:
                    NES_FATAL_ERROR("Parser::writeFieldValueToTupleBuffer: Field Type UNDEFINED");
            }
        }
        else if (physicalType->isTextType())
        {
            NES_TRACE(
                "Parser::writeFieldValueToTupleBuffer(): trying to write the variable length input string: {}"
                "to tuple buffer",
                inputString);
            tupleBuffer[tupleCount].writeVarSized(schemaFieldIndex, inputString, bufferProvider);
        }
        else
        { /// char array(string) case
            /// obtain pointer from buffer to fill with content via strcpy
            char* value = tupleBuffer[tupleCount][schemaFieldIndex].read<char*>();
            /// remove quotation marks from start and end of value (ASSUMES QUOTATIONMARKS AROUND STRINGS)
            /// improve behavior with json library
            strcpy(value, inputString.c_str());
        }
    }
    catch (const std::exception& e)
    {
        NES_ERROR("Failed to convert inputString to desired NES data type. Error: {}", e.what());
    }
}

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
    while (currentQueryStatus != Stopped && currentQueryStatus != Failed && currentQueryStatus != Finished)
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

void replaceFileSinkPath(SerializableDecomposedQueryPlan& decomposedQueryPlan, const std::string& fileName)
{
    EXPECT_EQ(decomposedQueryPlan.mutable_rootoperatorids()->size(), 1) << "Redirection is only implemented for Single Sink Queries";
    const auto rootOperatorId = decomposedQueryPlan.mutable_rootoperatorids()->at(0);
    auto& rootOperator = decomposedQueryPlan.mutable_operatormap()->at(rootOperatorId);

    EXPECT_TRUE(rootOperator.details().Is<SerializableOperator_SinkDetails>())
        << "Redirection expects the single root operator to be a sink operator";
    const auto deserializedSinkperator = OperatorSerializationUtil::deserializeOperator(rootOperator);
    auto descriptor
        = NES::Util::as_if<FileSinkDescriptor>(NES::Util::as<SinkLogicalOperator>(deserializedSinkperator)->getSinkDescriptor());
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

void replaceInputFileInSourceCSVs(SerializableDecomposedQueryPlan& decomposedQueryPlan, std::string newInputFileName)
{
    for (auto& pair : *decomposedQueryPlan.mutable_operatormap())
    {
        google::protobuf::uint64 key = pair.first;
        auto& value = pair.second; /// Note: non-const reference
        if (value.details().Is<SerializableOperator_SourceDescriptorLogicalOperator>())
        {
            auto deserializedSourceOperator = OperatorSerializationUtil::deserializeOperator(value);
            const auto sourceDescriptor
                = NES::Util::as<SourceDescriptorLogicalOperator>(deserializedSourceOperator)->getSourceDescriptorRef();
            if (sourceDescriptor.sourceType == Sources::SourceCSV::NAME)
            {
                /// We violate the immutability constrain of the SourceDescriptor here to patch in the correct file path.
                Configurations::DescriptorConfig::Config configUpdated = sourceDescriptor.config;
                configUpdated.at(Sources::ConfigParametersCSV::FILEPATH) = newInputFileName;
                auto sourceDescriptorUpdated = std::make_unique<Sources::SourceDescriptor>(
                    sourceDescriptor.schema,
                    sourceDescriptor.logicalSourceName,
                    sourceDescriptor.sourceType,
                    sourceDescriptor.inputFormat,
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

void replacePortInSourceTCPs(SerializableDecomposedQueryPlan& decomposedQueryPlan, const uint16_t mockTcpServerPort, const int sourceNumber)
{
    int queryPlanSourceTcpCounter = 0;
    for (auto& pair : *decomposedQueryPlan.mutable_operatormap())
    {
        google::protobuf::uint64 key = pair.first;
        auto& value = pair.second; /// Note: non-const reference
        if (value.details().Is<SerializableOperator_SourceDescriptorLogicalOperator>())
        {
            auto deserializedSourceOperator = OperatorSerializationUtil::deserializeOperator(value);
            const auto sourceDescriptor
                = NES::Util::as<SourceDescriptorLogicalOperator>(deserializedSourceOperator)->getSourceDescriptorRef();
            if (sourceDescriptor.sourceType == Sources::SourceTCP::NAME)
            {
                if (sourceNumber == queryPlanSourceTcpCounter)
                {
                    /// We violate the immutability constrain of the SourceDescriptor here to patch in the correct port.
                    Configurations::DescriptorConfig::Config configUpdated = sourceDescriptor.config;
                    configUpdated.at(Sources::ConfigParametersTCP::PORT) = static_cast<uint32_t>(mockTcpServerPort);
                    auto sourceDescriptorUpdated = std::make_unique<Sources::SourceDescriptor>(
                        sourceDescriptor.schema,
                        sourceDescriptor.logicalSourceName,
                        sourceDescriptor.sourceType,
                        sourceDescriptor.inputFormat,
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
                ++queryPlanSourceTcpCounter;
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
        = std::format("{}_{}_{}", std::string(test_info->test_suite_name()), std::string(test_info->name()), timestamp);
    std::ranges::replace(uniqueTestIdentifier, '/', '_');
    return uniqueTestIdentifier;
}
}

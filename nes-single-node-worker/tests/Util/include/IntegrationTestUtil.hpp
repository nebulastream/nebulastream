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

#pragma once

#include <memory>
#include <string>
#include <API/Schema.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <grpcpp/support/status.h>
#include <gtest/gtest-assertion-result.h>
#include <GrpcService.hpp>
#include <SerializableDecomposedQueryPlan.pb.h>

namespace NES::IntegrationTestUtil
{
static inline const std::string INPUT_CSV_FILES = "inputCSVFiles";

/// Creates multiple TupleBuffers from the csv file until the lastTimeStamp has been read
[[maybe_unused]] std::vector<Memory::TupleBuffer> createBuffersFromCSVFile(
    const std::string& csvFile,
    const std::shared_ptr<Schema>& schema,
    Memory::AbstractBufferProvider& bufferProvider,
    uint64_t originId = 0,
    const std::string& timestampFieldname = "ts",
    bool skipFirstLine = false);

/**
* @brief casts a value in string format to the correct type and writes it to the TupleBuffer
* @param value: string value that is cast to the PhysicalType and written to the TupleBuffer
* @param schemaFieldIndex: field/attribute that is currently processed
* @param tupleBuffer: the TupleBuffer to which the value is written containing the currently chosen memory layout
* @param json: denotes whether input comes from JSON for correct parsing
* @param schema: the schema the data are supposed to have
* @param tupleCount: current tuple count, i.e. how many tuples have already been produced
* @param bufferProvider: the buffer manager
*/
void writeFieldValueToTupleBuffer(
    std::string inputString,
    uint64_t schemaFieldIndex,
    Memory::MemoryLayouts::TestTupleBuffer& tupleBuffer,
    const std::shared_ptr<Schema>& schema,
    uint64_t tupleCount,
    Memory::AbstractBufferProvider& bufferProvider);

/// Loads the output @link Schema of the SinkOperator in the @link SerializableDecomposedQueryPlan. This requieres the plan to only
/// have a single root operator, which is the SinkOperator
std::shared_ptr<Schema> loadSinkSchema(SerializableDecomposedQueryPlan& queryPlan);

QueryId registerQueryPlan(const SerializableDecomposedQueryPlan& queryPlan, GRPCServer& uut);

void startQuery(QueryId queryId, GRPCServer& uut);

bool isQueryStopped(QueryId queryId, GRPCServer& uut);

void stopQuery(QueryId queryId, StopQueryRequest::QueryTerminationType type, GRPCServer& uut);

void unregisterQuery(QueryId queryId, GRPCServer& uut);

/// Summary structure of the query containing current status, number of retries and the exceptions.
QuerySummaryReply querySummary(QueryId queryId, GRPCServer& uut);

/// Expect query summary to fail for queryId with the following sc.
void querySummaryFailure(QueryId queryId, GRPCServer& uut, grpc::StatusCode statusCode);

/// Current status of the query.
QueryStatus queryStatus(QueryId queryId, GRPCServer& uut);

std::vector<std::pair<Runtime::Execution::QueryStatus, std::chrono::system_clock::time_point>> queryLog(QueryId queryId, GRPCServer& uut);

/// Wating for the query to end, e.g. by finishing or failing. Returns assertion success if the query ended (also if it failed).
testing::AssertionResult waitForQueryToEnd(QueryId queryId, GRPCServer& uut);

/// copies the input file to the directory where the test is executed to allow specifying only the input file name in
/// query instead of an absolute path or a relative path that is not the directory where the test is executed.
void copyInputFile(std::string_view inputFileName, std::string_view querySpecificDataFileName);

void removeFile(std::string_view filepath);

/// Loads a protobuf serialized @link SerializableDecomposedQueryPlan from a file in the TEST_DATA_DIR if possible.
bool loadFile(
    SerializableDecomposedQueryPlan& queryPlan,
    std::string_view queryFileName,
    std::string_view dataFileName,
    std::string_view querySpecificDataFileName);

/// Loads a protobuf serialized @link SerializableDecomposedQueryPlan from a file in the TEST_DATA_DIR if possible.
bool loadFile(SerializableDecomposedQueryPlan& queryPlan, const std::string_view queryFileName);

void replaceInputFileInFileSources(SerializableDecomposedQueryPlan& decomposedQueryPlan, std::string newInputFileName);

/// Iterates over a decomposed query plan and replaces all CSV sink file paths to ensure expected behavior.
void replaceFileSinkPath(SerializableDecomposedQueryPlan& decomposedQueryPlan, const std::string& filePathNew);

/// @brief Iterates over a decomposed query plan and replaces all sockets with the a free port generated for the mocked tcp server.
void replacePortInTCPSources(
    SerializableDecomposedQueryPlan& decomposedQueryPlan, const uint16_t mockTcpServerPort, const int sourceNumber);

std::string getUniqueTestIdentifier();


}

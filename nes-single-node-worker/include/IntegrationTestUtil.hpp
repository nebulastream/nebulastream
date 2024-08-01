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

#ifndef NES_SINGLE_NODE_WORKER_INCLUDE_INTEGRATIONTESTUTIL_HPP_
#define NES_SINGLE_NODE_WORKER_INCLUDE_INTEGRATIONTESTUTIL_HPP_

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <numeric>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/TCPSourceDescriptor.hpp>
#include <Operators/Serialization/OperatorSerializationUtil.hpp>
#include <Operators/Serialization/SchemaSerializationUtil.hpp>
#include <fmt/core.h>
#include <BaseIntegrationTest.hpp>
#include <GrpcService.hpp>
#include <SingleNodeWorkerRPCService.pb.h>


namespace NES
{
class IntegrationTestUtil
{
public:
    /**
     * Loads the output @link Schema of the SinkOperator in the @link SerializableDecomposedQueryPlan. This requieres the plan to only
     * have a single root operator, which is the SinkOperator
     * @param queryPlan the query plan that the sink schema is loaded from
     * @return output @link Schema
     */
    static SchemaPtr loadSinkSchema(SerializableDecomposedQueryPlan& queryPlan);

    static QueryId registerQueryPlan(const SerializableDecomposedQueryPlan& queryPlan, GRPCServer& uut);

    static void startQuery(QueryId queryId, GRPCServer& uut);

    static void stopQuery(QueryId queryId, QueryTerminationType type, GRPCServer& uut);

    static void unregisterQuery(QueryId queryId, GRPCServer& uut);

    /**
     * @brief copies the input file to the directory where the test is executed to allow specifying only the input file name in
     * query instead of an absolute path or a relative path that is not the directory where the test is executed.
     */
    static void copyInputFile(const std::string_view inputFileName);

    static void removeFile(const std::string_view filepath);

    /**
     * Loads a protobuf serialized @link SerializableDecomposedQueryPlan from a file in the TEST_DATA_DIR if possible.
     * @param queryPlan mutable ptr to a @link SerializableDecomposedQueryPlan
     * @param queryFileName name of the file in the TEST_DATA_DIR
     * @param dataFileName name of the file that the test reads data from
     * @return return false if the file could not be read or the query could not be parsed, otherwise returns true.
     */
    static bool
    loadFile(SerializableDecomposedQueryPlan& queryPlan, const std::string_view queryFileName, const std::string_view dataFileName);

    /**
     * Loads a protobuf serialized @link SerializableDecomposedQueryPlan from a file in the TEST_DATA_DIR if possible.
     * @param queryPlan mutable ptr to a @link SerializableDecomposedQueryPlan
     * @param queryFileName name of the file in the TEST_DATA_DIR
     * @return return false if the file could not be read or the query could not be parsed, otherwise returns true.
     */
    static bool loadFile(SerializableDecomposedQueryPlan& queryPlan, const std::string_view queryFileName);

    /**
     * Iterates over a decomposed query plan and replaces all CSV sink file paths to ensure expected behavior.
     * @param decomposedQueryPlan the decomposed query plan containing the csv file sink.
     * @param fileName the name that the csv file sink will write to.
     */
    static void replaceFileSinkPath(SerializableDecomposedQueryPlan& decomposedQueryPlan, const std::string_view fileName);


    /**
     * @brief Iterates over a decomposed query plan and replaces all sockets with the a free port generated for the mocked tcp server.
     * @param decomposedQueryPlan the decomposed query plan containing the tcp sources.
     * @param mockTcpServerPort the generated free socket used by the mocked tcp server.
     * @param sourceNumber the current source number for which a free port needs to be configured.
     */
    static void
    replacePortInTcpSources(SerializableDecomposedQueryPlan& decomposedQueryPlan, const uint16_t mockTcpServerPort, const int sourceNumber);
};

}

#endif /// NES_SINGLE_NODE_WORKER_INCLUDE_INTEGRATIONTESTUTIL_HPP_

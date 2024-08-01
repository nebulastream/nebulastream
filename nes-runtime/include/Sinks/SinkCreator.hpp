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

#ifndef NES_RUNTIME_INCLUDE_SINKS_SINKCREATOR_HPP_
#define NES_RUNTIME_INCLUDE_SINKS_SINKCREATOR_HPP_
#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES
{
/**
 * @brief create a csv test sink without a schema and append to existing file
 * @param schema of sink
 * @param bufferManager
 * @param path to file
 * @param bool indicating if data is appended (true) or overwritten (false)
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */

DataSinkPtr createCSVFileSink(
    const SchemaPtr& schema,
    SharedQueryId sharedQueryId,
    DecomposedQueryPlanId decomposedQueryPlanId,
    const Runtime::NodeEnginePtr& nodeEngine,
    uint32_t activeProducers,
    const std::string& filePath,
    bool append,
    bool addTimestamp = false,
    uint64_t numberOfOrigins = 1);

/**
 * @brief create a JSON test sink with a schema int
 * @param schema of sink
 * @param bufferManager
 * @param path to file
 * @param bool indicating if data is appended (true) or overwritten (false)
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */
DataSinkPtr createJSONFileSink(
    const SchemaPtr& schema,
    SharedQueryId sharedQueryId,
    DecomposedQueryPlanId decomposedQueryPlanId,
    const Runtime::NodeEnginePtr& nodeEngine,
    uint32_t numOfProducers,
    const std::string& filePath,
    bool append,
    uint64_t numberOfOrigins = 1);

/**
 * @brief create a print test sink with a schema
 * @param schema of sink
 * @param bufferManager
 * @param output stream
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */
DataSinkPtr createCsvPrintSink(
    const SchemaPtr& schema,
    SharedQueryId sharedQueryId,
    DecomposedQueryPlanId decomposedQueryPlanId,
    const Runtime::NodeEnginePtr& nodeEngine,
    uint32_t activeProducers,
    std::ostream& out,
    uint64_t numberOfOrigins = 1);

/**
 * @brief create a print test sink with a schema
 * @param schema of sink
 * @param parentPlan id of the parent qep
 * @param bufferManager
 * @param output stream
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */
DataSinkPtr createCSVPrintSink(
    const SchemaPtr& schema,
    SharedQueryId sharedQueryId,
    DecomposedQueryPlanId decomposedQueryPlanId,
    const Runtime::NodeEnginePtr& nodeEngine,
    uint32_t activeProducers,
    std::ostream& out,
    uint64_t numberOfOrigins = 1);

} /// namespace NES
#endif /// NES_RUNTIME_INCLUDE_SINKS_SINKCREATOR_HPP_

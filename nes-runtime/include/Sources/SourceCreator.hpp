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

#include <chrono>
#include <string>
#include <vector>
#include <stddef.h>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Sources/TCPSource.hpp>

#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/TCPSourceType.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Sources/DataSource.hpp>

namespace NES
{

/**
 * @brief function to create a csvfile source
 * @param schema schema of data source
 * @param bufferManager pointer to the buffer manager
 * @param queryManager pointer to the query manager
 * @param csvSourceType points to the current source configuration object
 * @param operatorId current operator id
 * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
 * @param numSourceLocalBuffers the number of buffers allocated to a source
 * @param physicalSourceName the name and unique identifier of a physical source
 * @param successors the subsequent operators in the pipeline to which the data is pushed
 * @return a data source pointer
 */
DataSourcePtr createCSVFileSource(
    const SchemaPtr& schema,
    const Runtime::BufferManagerPtr& bufferManager,
    const Runtime::QueryManagerPtr& queryManager,
    const CSVSourceTypePtr& csvSourceType,
    OperatorId operatorId,
    OriginId originId,
    size_t numSourceLocalBuffers,
    const std::string& physicalSourceName,
    const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

/**
 * function to create a TCP source
 * @param schema of this data source
 * @param bufferManager The BufferManager is responsible for: 1. Pooled Buffers: preallocated fixed-size buffers of memory that
 * must be reference counted 2. Unpooled Buffers: variable sized buffers that are allocated on-the-fly.
 * They are also subject to reference counting.
 * @param queryManager comes with functionality to manage the queries
 * @param tcpSourceType see TCPSourceType.hpp for information on this object
 * @param operatorId represents a locally running query execution plan
 * @param originId represents an origin
 * @param numSourceLocalBuffers number of local source buffers
 * @param physicalSourceName the name and unique identifier of a physical source
 * @param successors executable operators coming after this source
 * @return a data source pointer
 */
DataSourcePtr createTCPSource(
    const SchemaPtr& schema,
    const Runtime::BufferManagerPtr& bufferManager,
    const Runtime::QueryManagerPtr& queryManager,
    const TCPSourceTypePtr& tcpSourceType,
    OperatorId operatorId,
    OriginId originId,
    size_t numSourceLocalBuffers,
    const std::string& physicalSourceName,
    const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

} /// namespace NES

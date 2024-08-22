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

#include <string>
#include <stdint.h>
#include <Execution/Operators/Streaming/Join/StreamJoinBuildBucketing.hpp>

#include <Execution/Operators/Streaming/Join/StreamJoinBuild.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>

namespace NES
{
namespace Nautilus
{
class MemRef;
class Record;
class UInt64;
} /// namespace Nautilus
namespace QueryCompilation
{
enum class JoinBuildSideType : uint8_t;
} /// namespace QueryCompilation
} /// namespace NES

namespace NES::Runtime::Execution::Operators
{

/**
 * @brief This class implements how the nested loop join inserts records into the multiple buckets by implementing insertRecordForWindow.
 * It iterates over all windows, it has to fill and then inserts the record into each PagedVector of each window.
 */
class NLJBuildBucketing : public StreamJoinBuildBucketing
{
public:
    /**
     * @brief Constructor for a NLJBuildBucketing
     * @param operatorHandlerIndex
     * @param schema
     * @param joinFieldName
     * @param joinBuildSide
     * @param entrySize
     * @param timeFunction
     * @param joinStrategy
     * @param windowSize
     * @param windowSlide
     */
    NLJBuildBucketing(
        const uint64_t operatorHandlerIndex,
        const SchemaPtr& schema,
        const std::string& joinFieldName,
        const QueryCompilation::JoinBuildSideType joinBuildSide,
        const uint64_t entrySize,
        TimeFunctionPtr timeFunction,
        QueryCompilation::StreamJoinStrategy joinStrategy,
        const uint64_t windowSize,
        const uint64_t windowSlide);

    void insertRecordForWindow(
        Value<MemRef>& allWindowsToFill, Value<UInt64>& curIndex, ValueId<WorkerThreadId>& workerThreadId, Record& record) const override;
};
} /// namespace NES::Runtime::Execution::Operators

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

#include <Sinks/Sink.hpp>


#include <memory>
#include <variant>

namespace NES
{
struct AbstractQueryStatusListener;
using AbstractQueryStatusListenerPtr = std::shared_ptr<AbstractQueryStatusListener>;

namespace Configurations
{

class WorkerConfiguration;
using WorkerConfigurationPtr = std::shared_ptr<WorkerConfiguration>;

} /// namespace Configurations

enum class PipelineStageArity : uint8_t
{
    Unary,
    BinaryLeft,
    BinaryRight
};

class PhysicalType;
using PhysicalTypePtr = std::shared_ptr<PhysicalType>;

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class SourceData;

namespace Runtime
{

class BufferStorage;
using BufferStoragePtr = std::shared_ptr<BufferStorage>;

class PhysicalField;
using PhysicalFieldPtr = std::shared_ptr<PhysicalField>;
template <class ValueType>
class BasicPhysicalField;
class ArrayPhysicalField;

class PhysicalSchema;
using PhysicalSchemaPtr = std::shared_ptr<PhysicalSchema>;

class OpenCLManager;
using OpenCLManagerPtr = std::shared_ptr<OpenCLManager>;

class WorkerContext;
using WorkerContextRef = WorkerContext&;

class NodeEngine;
using NodeEnginePtr = std::shared_ptr<NodeEngine>;

class QueryManager;
using QueryManagerPtr = std::shared_ptr<QueryManager>;

class QueryStatistics;
using QueryStatisticsPtr = std::shared_ptr<QueryStatistics>;

class ReconfigurationMessage;

namespace Execution
{

class OperatorHandler;
using OperatorHandlerPtr = std::shared_ptr<OperatorHandler>;

enum class OperatorHandlerType : uint8_t
{
    WINDOW,
    CEP,
    JOIN,
    BATCH_JOIN,
    KEY_EVENT_TIME_WINDOW
};

class ExecutablePipeline;
using ExecutablePipelinePtr = std::shared_ptr<ExecutablePipeline>;

class ExecutableQueryPlan;
using ExecutableQueryPlanPtr = std::shared_ptr<ExecutableQueryPlan>;

/// Todo: can the sink pointer be unique?
namespace Sinks
{
class Sink;
}
using SuccessorExecutablePipeline = std::variant<std::shared_ptr<NES::Sinks::Sink>, ExecutablePipelinePtr>;
using PredecessorExecutablePipeline = std::variant<std::weak_ptr<SourceData>, std::weak_ptr<ExecutablePipeline>>;

class ExecutablePipelineStage;
using ExecutablePipelineStagePtr = std::shared_ptr<ExecutablePipelineStage>;

class PipelineExecutionContext;
using PipelineExecutionContextPtr = std::shared_ptr<PipelineExecutionContext>;

} /// namespace Execution

namespace MemoryLayouts
{

class MemoryLayout;
using MemoryLayoutPtr = std::shared_ptr<MemoryLayout>;

class ColumnLayout;
using ColumnLayoutPtr = std::shared_ptr<ColumnLayout>;

class RowLayout;
using RowLayoutPtr = std::shared_ptr<RowLayout>;

} /// namespace MemoryLayouts

} /// namespace Runtime

namespace QueryCompilation
{
class QueryCompiler;
using QueryCompilerPtr = std::shared_ptr<QueryCompiler>;
struct QueryCompilerOptions;
} /// namespace QueryCompilation

} /// namespace NES

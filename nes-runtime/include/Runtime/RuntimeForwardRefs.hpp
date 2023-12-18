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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_RUNTIMEFORWARDREFS_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_RUNTIMEFORWARDREFS_HPP_

#include <memory>
#include <string>
#include <variant>

/**
 * @brief this alias represent a locally running query execution plan
 */
using QuerySubPlanId = uint64_t;
using OriginId = uint64_t;

namespace NES {
#ifdef UNIKERNEL_LIB
namespace Unikernel {
class UnikernelPipelineExecutionContext;
}
#endif
class AbstractQueryStatusListener;
using AbstractQueryStatusListenerPtr = std::shared_ptr<AbstractQueryStatusListener>;

namespace Configurations {

class WorkerConfiguration;
using WorkerConfigurationPtr = std::shared_ptr<WorkerConfiguration>;

}// namespace Configurations

enum class PipelineStageArity : uint8_t { Unary, BinaryLeft, BinaryRight };

class PhysicalType;
using PhysicalTypePtr = std::shared_ptr<PhysicalType>;

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class SinkMedium;
using DataSinkPtr = std::shared_ptr<SinkMedium>;

class DataSource;
using DataSourcePtr = std::shared_ptr<DataSource>;

class DataEmitter;
#ifndef UNIKERNEL_LIB
using DataEmitterPtr = std::shared_ptr<DataEmitter>;
#else
using DataEmitterPtr = DataEmitter*;
#endif

namespace Runtime {

enum class NumaAwarenessFlag : uint8_t { ENABLED, DISABLED };

class RuntimeEventListener;
#ifndef UNIKERNEL_LIB
using RuntimeEventListenerPtr = std::shared_ptr<RuntimeEventListener>;
#else
using RuntimeEventListenerPtr =RuntimeEventListener*;
#endif

class BufferStorage;
using BufferStoragePtr = std::shared_ptr<BufferStorage>;

class PhysicalField;
using PhysicalFieldPtr = std::shared_ptr<PhysicalField>;
template<class ValueType>
class BasicPhysicalField;
class ArrayPhysicalField;

class PhysicalSchema;
using PhysicalSchemaPtr = std::shared_ptr<PhysicalSchema>;

class TupleBuffer;

class HardwareManager;
using HardwareManagerPtr = std::shared_ptr<HardwareManager>;

class BufferManager;
using BufferManagerPtr = std::shared_ptr<BufferManager>;

class LocalBufferPool;
using LocalBufferPoolPtr = std::shared_ptr<LocalBufferPool>;

class FixedSizeBufferPool;
using FixedSizeBufferPoolPtr = std::shared_ptr<FixedSizeBufferPool>;

class WorkerContext;
using WorkerContextRef = WorkerContext&;
#ifndef UNIKERNEL_LIB
using WorkerContextPtr = std::shared_ptr<WorkerContext>;
#else
using WorkerContextPtr = WorkerContext*;
#endif

class NodeEngine;
using NodeEnginePtr = std::shared_ptr<NodeEngine>;

class AbstractQueryManager;
using QueryManagerPtr = std::shared_ptr<AbstractQueryManager>;

class StateManager;
using StateManagerPtr = std::shared_ptr<StateManager>;

class QueryStatistics;
using QueryStatisticsPtr = std::shared_ptr<QueryStatistics>;

class ReconfigurationMessage;

namespace Execution {

class OperatorHandler;
using OperatorHandlerPtr = std::shared_ptr<OperatorHandler>;

enum class OperatorHandlerType : uint8_t { WINDOW, CEP, JOIN, BATCH_JOIN, KEY_EVENT_TIME_WINDOW };

class ExecutablePipeline;
using ExecutablePipelinePtr = std::shared_ptr<ExecutablePipeline>;

class ExecutableQueryPlan;
using ExecutableQueryPlanPtr = std::shared_ptr<ExecutableQueryPlan>;

using SuccessorExecutablePipeline = std::variant<DataSinkPtr, ExecutablePipelinePtr>;
using PredecessorExecutablePipeline = std::variant<std::weak_ptr<DataSource>, std::weak_ptr<ExecutablePipeline>>;

class ExecutablePipelineStage;
using ExecutablePipelineStagePtr = std::shared_ptr<ExecutablePipelineStage>;

#ifndef UNIKERNEL_LIB
class PipelineExecutionContext;
using PipelineExecutionContextPtr = std::shared_ptr<PipelineExecutionContext>;
#else
using PipelineExecutionContext = NES::Unikernel::UnikernelPipelineExecutionContext;
using PipelineExecutionContextPtr = NES::Unikernel::UnikernelPipelineExecutionContext*;
#endif

}// namespace Execution

namespace MemoryLayouts {

class MemoryLayout;
using MemoryLayoutPtr = std::shared_ptr<MemoryLayout>;

class ColumnLayout;
using ColumnLayoutPtr = std::shared_ptr<ColumnLayout>;

class RowLayout;
using RowLayoutPtr = std::shared_ptr<RowLayout>;

}// namespace MemoryLayouts

}// namespace Runtime

namespace QueryCompilation {
class QueryCompiler;
using QueryCompilerPtr = std::shared_ptr<QueryCompiler>;
class QueryCompilerOptions;
using QueryCompilerOptionsPtr = std::shared_ptr<QueryCompilerOptions>;
}// namespace QueryCompilation

}// namespace NES

#endif// NES_RUNTIME_INCLUDE_RUNTIME_RUNTIMEFORWARDREFS_HPP_

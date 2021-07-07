/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_NODEENGINE_NODEENGINEFORWAREDREFS_HPP_
#define NES_INCLUDE_NODEENGINE_NODEENGINEFORWAREDREFS_HPP_
#include <memory>
#include <variant>

namespace NES {
class NodeStats;
enum PipelineStageArity : uint8_t { Unary, BinaryLeft, BinaryRight };

class PhysicalType;
using PhysicalTypePtr = std::shared_ptr<PhysicalType>;

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class SinkMedium;
using DataSinkPtr = std::shared_ptr<SinkMedium>;

class DataSource;
using DataSourcePtr = std::shared_ptr<DataSource>;

namespace Runtime {

class TupleBuffer;

class PhysicalField;
using PhysicalFieldPtr = std::shared_ptr<PhysicalField>;
template<class ValueType>
class BasicPhysicalField;
class ArrayPhysicalField;

class PhysicalSchema;
using PhysicalSchemaPtr = std::shared_ptr<PhysicalSchema>;

class TupleBuffer;

class BufferManager;
using BufferManagerPtr = std::shared_ptr<BufferManager>;

class LocalBufferPool;
using LocalBufferPoolPtr = std::shared_ptr<LocalBufferPool>;

class FixedSizeBufferPool;
using FixedSizeBufferPoolPtr = std::shared_ptr<FixedSizeBufferPool>;

class WorkerContext;
using WorkerContextRef = WorkerContext&;

class NodeStatsProvider;
using NodeStatsProviderPtr = std::shared_ptr<NodeStatsProvider>;

class NodeEngine;
using NodeEnginePtr = std::shared_ptr<NodeEngine>;

class QueryManager;
using QueryManagerPtr = std::shared_ptr<QueryManager>;

class StateManager;
using StateManagerPtr = std::shared_ptr<StateManager>;

class QueryStatistics;
using QueryStatisticsPtr = std::shared_ptr<QueryStatistics>;

class ReconfigurationMessage;

namespace Execution {

class OperatorHandler;
using OperatorHandlerPtr = std::shared_ptr<OperatorHandler>;

class ExecutablePipeline;
using ExecutablePipelinePtr = std::shared_ptr<ExecutablePipeline>;

class ExecutableQueryPlan;
using ExecutableQueryPlanPtr = std::shared_ptr<ExecutableQueryPlan>;

using SuccessorExecutablePipeline = std::variant<DataSinkPtr, ExecutablePipelinePtr>;
using PredecessorExecutablePipeline = std::variant<std::weak_ptr<DataSource>, std::weak_ptr<ExecutablePipeline>>;

class ExecutablePipelineStage;
using ExecutablePipelineStagePtr = std::shared_ptr<ExecutablePipelineStage>;

class PipelineExecutionContext;
using PipelineExecutionContextPtr = std::shared_ptr<PipelineExecutionContext>;

}// namespace Execution

namespace DynamicMemoryLayout {

class DynamicMemoryLayout;
using DynamicMemoryLayoutPtr = std::shared_ptr<DynamicMemoryLayout>;

class DynamicColumnLayout;
using DynamicColumnLayoutPtr = std::shared_ptr<DynamicColumnLayout>;

class DynamicRowLayout;
using DynamicRowLayoutPtr = std::shared_ptr<DynamicRowLayout>;

class DynamicColumnLayoutBuffer;
using DynamicColumnLayoutBufferPtr = std::shared_ptr<DynamicColumnLayoutBuffer>;

class DynamicRowLayoutBuffer;
using DynamicRowLayoutBufferPtr = std::shared_ptr<DynamicRowLayoutBuffer>;

}// namespace DynamicMemoryLayout

}// namespace Runtime

namespace QueryCompilation {
class QueryCompiler;
using QueryCompilerPtr = std::shared_ptr<QueryCompiler>;
}// namespace QueryCompilation

}// namespace NES

#endif//NES_INCLUDE_NODEENGINE_NODEENGINEFORWAREDREFS_HPP_

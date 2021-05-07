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

enum PipelineStageArity : uint8_t { Unary, BinaryLeft, BinaryRight };

class PhysicalType;
typedef std::shared_ptr<PhysicalType> PhysicalTypePtr;

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class SinkMedium;
typedef std::shared_ptr<SinkMedium> DataSinkPtr;

class DataSource;
typedef std::shared_ptr<DataSource> DataSourcePtr;

namespace NodeEngine {

class TupleBuffer;

class PhysicalField;
typedef std::shared_ptr<PhysicalField> PhysicalFieldPtr;
template<class ValueType>
class BasicPhysicalField;
class ArrayPhysicalField;

class PhysicalSchema;
typedef std::shared_ptr<PhysicalSchema> PhysicalSchemaPtr;

class MemoryLayout;
typedef std::shared_ptr<MemoryLayout> MemoryLayoutPtr;

class TupleBuffer;

class BufferManager;
typedef std::shared_ptr<BufferManager> BufferManagerPtr;

class LocalBufferPool;
typedef std::shared_ptr<LocalBufferPool> LocalBufferPoolPtr;

class FixedSizeBufferPool;
typedef std::shared_ptr<FixedSizeBufferPool> FixedSizeBufferPoolPtr;

class WorkerContext;
typedef WorkerContext& WorkerContextRef;

class NodeStatsProvider;
typedef std::shared_ptr<NodeStatsProvider> NodeStatsProviderPtr;

class NodeEngine;
typedef std::shared_ptr<NodeEngine> NodeEnginePtr;

class QueryManager;
typedef std::shared_ptr<QueryManager> QueryManagerPtr;

class StateManager;
typedef std::shared_ptr<StateManager> StateManagerPtr;

class QueryStatistics;
typedef std::shared_ptr<QueryStatistics> QueryStatisticsPtr;

class ReconfigurationMessage;

namespace Execution {

class OperatorHandler;
typedef std::shared_ptr<OperatorHandler> OperatorHandlerPtr;

class ExecutableQueryPlan;
typedef std::shared_ptr<ExecutableQueryPlan> ExecutableQueryPlanPtr;

class ExecutablePipeline;
typedef std::shared_ptr<ExecutablePipeline> ExecutablePipelinePtr;

class NewExecutablePipeline;
typedef std::shared_ptr<NewExecutablePipeline> NewExecutablePipelinePtr;

class NewExecutableQueryPlan;
typedef std::shared_ptr<NewExecutableQueryPlan> NewExecutableQueryPlanPtr;

typedef std::variant<DataSinkPtr, NewExecutablePipelinePtr> SuccessorExecutablePipeline;
typedef std::variant<std::weak_ptr<DataSource>, std::weak_ptr<NewExecutablePipeline>> PredecessorExecutablePipeline;

class ExecutablePipelineStage;
typedef std::shared_ptr<ExecutablePipelineStage> ExecutablePipelineStagePtr;

class PipelineExecutionContext;
typedef std::shared_ptr<PipelineExecutionContext> PipelineExecutionContextPtr;

}// namespace Execution

namespace DynamicMemoryLayout {
class DynamicRowLayoutBuffer;
typedef std::shared_ptr<DynamicRowLayoutBuffer> DynamicRowLayoutBufferPtr;

class DynamicColumnLayoutBuffer;
typedef std::shared_ptr<DynamicColumnLayoutBuffer> DynamicColumnLayoutBufferPtr;

class DynamicMemoryLayout;
typedef std::shared_ptr<DynamicMemoryLayout> DynamicMemoryLayoutPtr;

class DynamicColumnLayout;
typedef std::shared_ptr<DynamicColumnLayout> DynamicColumnLayoutPtr;

class DynamicRowLayout;
typedef std::shared_ptr<DynamicRowLayout> DynamicRowLayoutPtr;

}// namespace DynamicMemoryLayout

}// namespace NodeEngine

namespace QueryCompilation{
class QueryCompiler;
typedef std::shared_ptr<QueryCompiler> QueryCompilerPtr;
}

}// namespace NES

#endif//NES_INCLUDE_NODEENGINE_NODEENGINEFORWAREDREFS_HPP_

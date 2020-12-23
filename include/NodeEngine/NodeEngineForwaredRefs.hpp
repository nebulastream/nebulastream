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

namespace NES {

enum PipelineStageArity : uint8_t { Unary, BinaryLeft, BinaryRight };

class PhysicalType;
typedef std::shared_ptr<PhysicalType> PhysicalTypePtr;

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

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

class WorkerContext;
typedef WorkerContext& WorkerContextRef;

class NodeStatsProvider;
typedef std::shared_ptr<NodeStatsProvider> NodeStatsProviderPtr;

class NodeEngine;
typedef std::shared_ptr<NodeEngine> NodeEnginePtr;

class QueryManager;
typedef std::shared_ptr<QueryManager> QueryManagerPtr;

class QueryStatistics;
typedef std::shared_ptr<QueryStatistics> QueryStatisticsPtr;

class ReconfigurationTask;

namespace Execution {

class OperatorHandler;
typedef std::shared_ptr<OperatorHandler> OperatorHandlerPtr;

class ExecutableQueryPlan;
typedef std::shared_ptr<ExecutableQueryPlan> ExecutableQueryPlanPtr;

class ExecutablePipeline;
typedef std::shared_ptr<ExecutablePipeline> ExecutablePipelinePtr;

class ExecutablePipelineStage;
typedef std::shared_ptr<ExecutablePipelineStage> ExecutablePipelineStagePtr;

class PipelineExecutionContext;
typedef std::shared_ptr<PipelineExecutionContext> PipelineExecutionContextPtr;

}// namespace Execution

}// namespace NodeEngine

}// namespace NES

#endif//NES_INCLUDE_NODEENGINE_NODEENGINEFORWAREDREFS_HPP_

#ifndef NES_QUERYCOMPILER_HPP
#define NES_QUERYCOMPILER_HPP
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Execution/Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Identifiers.hpp>
#include <OperatorHandlerTracer.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <map>
#include <vector>

using SharedOperatorHandlerId = size_t;
using SharedOperatorHandlers = std::map<SharedOperatorHandlerId, NES::Runtime::Unikernel::OperatorHandlerDescriptor>;
using EitherSharedOrLocal = std::variant<SharedOperatorHandlerId, NES::Runtime::Unikernel::OperatorHandlerDescriptor>;
struct Stage {
    uint64_t pipelineId;
    std::optional<NES::PipelineId> successor;
    std::vector<NES::PipelineId> predecessors;
    std::unique_ptr<NES::Runtime::Execution::CompiledExecutablePipelineStage> stage;
    std::vector<EitherSharedOrLocal> handlers;
};

struct StageWithHandlers {
    uint64_t pipelineId;
    std::optional<NES::PipelineId> successor;
    std::vector<NES::PipelineId> predecessors;
    std::unique_ptr<NES::Runtime::Execution::CompiledExecutablePipelineStage> stage;
    std::vector<NES::Runtime::Execution::OperatorHandlerPtr> handlers;
};

struct SinkStage {
    NES::PipelineId id;
    std::vector<NES::PipelineId> predecessor;
    NES::Network::NetworkSinkDescriptorPtr descriptor;
};

class ExportQueryCompiler {
  public:
    std::tuple<std::map<NES::PipelineId, SinkStage>,
               std::map<NES::PipelineId, std::pair<NES::SourceDescriptorPtr, NES::OriginId>>,
               std::vector<Stage>,
               std::vector<NES::Runtime::Unikernel::OperatorHandlerDescriptor>>
    compile(NES::QueryPlanPtr queryPlan, size_t bufferSize, NES::Catalogs::Source::SourceCatalogPtr sourceCatalog);
};

#endif//NES_QUERYCOMPILER_HPP

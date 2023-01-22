//
// Created by kasper on 1/16/23.
//

#ifndef NES_DECORATEDLORAWANNODEENGINE_HPP
#define NES_DECORATEDLORAWANNODEENGINE_HPP

#include <Sources/LoRaWANProxySource.hpp>
#include <Runtime/NodeEngine.hpp>

namespace NES::Runtime {

class DecoratedLoRaWANNodeEngine: public NodeEngine
{
  public:
    explicit DecoratedLoRaWANNodeEngine(std::vector<PhysicalSourcePtr> physicalSources,
                               HardwareManagerPtr&& hardwareManager,
                               std::vector<BufferManagerPtr>&& bufferManagers,
                               QueryManagerPtr&& queryManager,
                               std::function<Network::NetworkManagerPtr(std::shared_ptr<NodeEngine>)>&& networkManagerCreator,
                               Network::PartitionManagerPtr&& partitionManager,
                               QueryCompilation::QueryCompilerPtr&& queryCompiler,
                               StateManagerPtr&& stateManager,
                               std::weak_ptr<AbstractQueryStatusListener>&& nesWorker,
                               NES::Experimental::MaterializedView::MaterializedViewManagerPtr&& materializedViewManager,
                               uint64_t nodeEngineId,
                               uint64_t numberOfBuffersInGlobalBufferManager,
                               uint64_t numberOfBuffersInSourceLocalBufferPool,
                               uint64_t numberOfBuffersPerWorker,
                               bool sourceSharing,
                               LoRaWANProxySourceTypePtr source);

    /**
     * @brief registers a query
     * TODO: since this doesn't override it only hides the function from the parent class.
     * This means any internal call from the parent class will refer to the parent method. not the child.
     * Possibly not an issue
     * @param queryId: id of the query sub plan to be registered
     * @param queryExecutionId: query execution plan id
     * @param operatorTree: query sub plan to register
     * @return true if succeeded, else false
     */
    [[nodiscard]] bool registerQueryInNodeEngine(const QueryPlanPtr&);

    [[nodiscard]] bool unregisterQuery(QueryId);

  private:
    LoRaWANProxySourceTypePtr sourceType;
};

}

#endif//NES_DECORATEDLORAWANNODEENGINE_HPP

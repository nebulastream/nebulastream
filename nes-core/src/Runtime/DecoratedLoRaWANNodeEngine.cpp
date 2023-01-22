//
// Created by kasper on 1/16/23.
//

#include <Runtime/DecoratedLoRaWANNodeEngine.hpp>
#include <GRPC/Serialization/EndDeviceProtocolSerializationUtil.hpp>
#include <utility>
namespace NES::Runtime {
DecoratedLoRaWANNodeEngine::DecoratedLoRaWANNodeEngine(
    std::vector<PhysicalSourcePtr> physicalSources,
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
    LoRaWANProxySourceTypePtr proxysource)
    : NodeEngine(std::move(physicalSources),
                 std::move(hardwareManager),
                 std::move(bufferManagers),
                 std::move(queryManager),
                 std::move(networkManagerCreator),
                 std::move(partitionManager),
                 std::move(queryCompiler),
                 std::move(stateManager),
                 std::move(nesWorker),
                 std::move(materializedViewManager),
                 nodeEngineId,
                 numberOfBuffersInGlobalBufferManager,
                 numberOfBuffersInSourceLocalBufferPool,
                 numberOfBuffersPerWorker,
                 sourceSharing), sourceType(std::move(proxysource)){
    NES_DEBUG("DECORATEDLORAWANCLASS CREATED")
}
bool DecoratedLoRaWANNodeEngine::registerQueryInNodeEngine(const NES::QueryPlanPtr& queryPlan) {
    NES_DEBUG("DECORATED FUNCTION CALLED")
    auto serQuery = EndDeviceProtocolSerializationUtil::serializeQueryPlanToEndDevice(queryPlan);
    sourceType->addSerializedQuery(queryPlan->getQueryId(), serQuery);
    return NodeEngine::registerQueryInNodeEngine(queryPlan);
}

bool DecoratedLoRaWANNodeEngine::unregisterQuery(QueryId queryId) {
    sourceType->removeSerializedQuery(queryId);
    return NodeEngine::unregisterQuery(queryId);
}
}// namespace NES::Runtime

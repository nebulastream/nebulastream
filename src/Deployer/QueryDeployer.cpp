#include <API/Query.hpp>
#include <Catalogs/QueryCatalog.hpp>
#include <Deployer/QueryDeployer.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyManager.hpp>

namespace NES {

QueryDeployer::QueryDeployer(QueryCatalogPtr queryCatalog, TopologyManagerPtr topologyManager, GlobalExecutionPlanPtr executionPlan)
    : queryCatalog(queryCatalog), topologyManager(topologyManager), executionPlan(executionPlan) {
    NES_INFO("QueryDeployer()");
}

QueryDeployer::~QueryDeployer() {
    NES_INFO("~QueryDeployer()");
    queryToPort.clear();
}

std::vector<ExecutionNodePtr> QueryDeployer::generateDeployment(const string& queryId) {
    if (queryCatalog->queryExists(queryId)
        && !queryCatalog->isQueryRunning(queryId)) {
        NES_INFO("QueryDeployer:: generateDeployment for query " << queryId);

        const auto executionNodes = executionPlan->getExecutionNodesByQueryId(queryId);
        //iterate through all vertices in the topology
        for (ExecutionNodePtr executionNode : executionNodes) {
            QueryPlanPtr querySubPlan = executionNode->getQuerySubPlan(queryId);
            //Update port information for the system generated source and sink operators
            const auto sourceOperators = querySubPlan->getSourceOperators();
            for (auto sourceOperator : sourceOperators) {
                if (sourceOperator->getId()) {
                    auto zmqDescriptor = sourceOperator->getSourceDescriptor()->as<ZmqSourceDescriptor>();
                    zmqDescriptor->setPort(assignPort(queryId));
                }
            }

            const auto sinkOperators = querySubPlan->getSinkOperators();
            for (auto sinkOperator : sinkOperators) {
                if (sinkOperator->getId()) {
                    auto zmqDescriptor = sinkOperator->getSinkDescriptor()->as<ZmqSinkDescriptor>();
                    zmqDescriptor->setPort(assignPort(queryId));
                }
            }
        }

        queryCatalog->markQueryAs(queryId, QueryStatus::Scheduling);
        NES_INFO("QueryDeployer::generateDeployment: prepareExecutableTransferObject successfully " << queryId);
        return executionNodes;
    } else if (queryCatalog->getQuery(queryId)->getQueryStatus() == QueryStatus::Running) {
        NES_WARNING("QueryDeployer::generateDeployment: Query is already running -> " << queryId);
    } else {
        NES_WARNING("QueryDeployer::generateDeployment: Query is not registered -> " << queryId);
    }

    return {};
}

int QueryDeployer::assignPort(const string& queryId) {
    if (this->queryToPort.find(queryId) != this->queryToPort.end()) {
        NES_DEBUG("QueryDeployer::assignPort query found with id=" << queryId << " return port=" << this->queryToPort.at(queryId));
        return this->queryToPort.at(queryId);
    } else {
        // increase max port in map by 1
        const NESTopologyEntryPtr kRootNode = topologyManager->getRootNode();
        uint16_t kFreeZmqPort = kRootNode->getNextFreeReceivePort();
        this->queryToPort.insert({queryId, kFreeZmqPort});
        NES_DEBUG("CoordinatorService::assignPort create a new port for query id=" << queryId << " port=" << kFreeZmqPort);
        return kFreeZmqPort;
    }
}

}// namespace NES

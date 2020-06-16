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

QueryDeployer::QueryDeployer(QueryCatalogPtr queryCatalog, TopologyManagerPtr topologyManager, GlobalExecutionPlanPtr globalExecutionPlan)
    : queryCatalog(queryCatalog), topologyManager(topologyManager), globalExecutionPlan(globalExecutionPlan) {
    NES_INFO("QueryDeployer()");
}

QueryDeployer::~QueryDeployer() {
    NES_INFO("~QueryDeployer()");
    queryToPort.clear();
}

bool QueryDeployer::prepareForDeployment(const string& queryId) {
    if (queryCatalog->queryExists(queryId) && !queryCatalog->isQueryRunning(queryId)) {
        NES_INFO("QueryDeployer:: prepareForDeployment for query " << queryId);
        const auto executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
        //iterate through all vertices in the topology
        for (ExecutionNodePtr executionNode : executionNodes) {
            QueryPlanPtr querySubPlan = executionNode->getQuerySubPlan(queryId);
            if (!querySubPlan) {
                NES_WARNING("NesCoordinator : unable to find query sub plan with id " << queryId);
                return false;
            }
            NES_DEBUG("QueryDeployer: Update port for system generated source operators for query " << queryId);
            //Update port information for the system generated source and sink operators
            const auto sourceOperators = querySubPlan->getSourceOperators();
            for (auto sourceOperator : sourceOperators) {
                //The operator Id is used to identify the system generated source operator
                if (sourceOperator->getId() == SYS_SOURCE_OPERATOR_ID) {
                    auto zmqDescriptor = sourceOperator->getSourceDescriptor()->as<ZmqSourceDescriptor>();
                    zmqDescriptor->setPort(assignPort(queryId));
                }
            }

            NES_DEBUG("QueryDeployer: Update port for system generated sink operators for query " << queryId);
            const auto sinkOperators = querySubPlan->getSinkOperators();
            for (auto sinkOperator : sinkOperators) {
                //The operator Id is used to identify the system generated sink operator
                if (sinkOperator->getId() == SYS_SINK_OPERATOR_ID) {
                    auto zmqDescriptor = sinkOperator->getSinkDescriptor()->as<ZmqSinkDescriptor>();
                    zmqDescriptor->setPort(assignPort(queryId));
                }
            }
        }
        queryCatalog->markQueryAs(queryId, QueryStatus::Scheduling);
        NES_INFO("QueryDeployer::prepareForDeployment: prepareExecutableTransferObject successfully " << queryId);
        return true;
    } else if (queryCatalog->getQuery(queryId)->getQueryStatus() == QueryStatus::Running) {
        NES_WARNING("QueryDeployer::prepareForDeployment: Query is already running -> " << queryId);
        return false;
    } else {
        NES_WARNING("QueryDeployer::prepareForDeployment: Query is not registered -> " << queryId);
        return false;
    }
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

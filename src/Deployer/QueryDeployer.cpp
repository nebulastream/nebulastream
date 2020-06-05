#include <Catalogs/QueryCatalog.hpp>
#include <Deployer/QueryDeployer.hpp>
#include <GRPC/ExecutableTransferObject.hpp>
#include <Operators/Impl/SinkOperator.hpp>
#include <Operators/Impl/SourceOperator.hpp>
#include <Optimizer/NESExecutionPlan.hpp>
#include <SourceSink/DataSink.hpp>
#include <SourceSink/DataSource.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <Topology/TopologyManager.hpp>
#include <string>
#include <API/Query.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>

namespace NES {

QueryDeployer::QueryDeployer(QueryCatalogPtr queryCatalog, TopologyManagerPtr topologyManager) : queryCatalog(queryCatalog),
                                                                                                 topologyManager(topologyManager) {
    NES_INFO("QueryDeployer()");
}

QueryDeployer::~QueryDeployer() {
    NES_INFO("~QueryDeployer()");
    queryToPort.clear();
}

map<NESTopologyEntryPtr, ExecutableTransferObject> QueryDeployer::generateDeployment(const string& queryId) {
    map<NESTopologyEntryPtr, ExecutableTransferObject> output;
    if (queryCatalog->queryExists(queryId)
        && !queryCatalog->isQueryRunning(queryId)) {
        NES_INFO("QueryDeployer::generateDeployment for query " << queryId);

        NESExecutionPlanPtr execPlan = queryCatalog->getQuery(queryId)->getNesPlanPtr();
        SchemaPtr schema = queryCatalog->getQuery(queryId)->getQueryPlan()->getRootOperator()->getNodesByType<SourceLogicalOperatorNode>()[0]->getOutputSchema();
        //iterate through all vertices in the topology
        for (const ExecutionVertex& v : execPlan->getExecutionGraph()->getAllVertex()) {
            OperatorPtr operators = v.ptr->getRootOperator();
            if (operators) {
                // if node contains operators to be deployed -> serialize and send them to the according node
                vector<DataSourcePtr> sources = getSources(queryId, v);
                vector<DataSinkPtr> destinations = getSinks(queryId, v);
                NESTopologyEntryPtr nesNode = v.ptr->getNESNode();
                //TODO: this will break for multiple streams
                ExecutableTransferObject eto = ExecutableTransferObject(queryId, schema,
                                                                        sources,
                                                                        destinations,
                                                                        operators);
                output.insert({nesNode, eto});
            }
        }

        queryCatalog->markQueryAs(queryId, QueryStatus::Scheduling);

    } else if (queryCatalog->getQuery(queryId)->getQueryStatus() == QueryStatus::Running) {
        NES_WARNING("QueryDeployer::generateDeployment: Query is already running -> " << queryId);
    } else {
        NES_WARNING("QueryDeployer::generateDeployment: Query is not registered -> " << queryId);
    }

    NES_INFO("QueryDeployer::generateDeployment: prepareExecutableTransferObject successfully " << queryId);

    return output;
}

vector<DataSourcePtr> QueryDeployer::getSources(const string& queryId,
                                                const ExecutionVertex& v) {
    NES_DEBUG("QueryDeployer::getSources: queryid=" << queryId << " vertex=" << v.id);
    vector<DataSourcePtr> sources = vector<DataSourcePtr>();
    SchemaPtr schema = queryCatalog->getQuery(queryId)->getQueryPlan()->getRootOperator()->getOutputSchema();

    DataSourcePtr source = findDataSourcePointer(v.ptr->getRootOperator());

    if (source->getType() == ZMQ_SOURCE) {
        const NESTopologyEntryPtr kRootNode = topologyManager->getRootNode();

        //TODO: this does not work this way, replace it here with the descriptor
        BufferManagerPtr bPtr;
        QueryManagerPtr disPtr;
        source = createZmqSource(schema, bPtr, disPtr, kRootNode->getIp(), assign_port(queryId));
    }
    sources.emplace_back(source);
    NES_DEBUG("QueryDeployer::generateDeployment getSources: " << source->toString());

    return sources;
}

vector<DataSinkPtr> QueryDeployer::getSinks(const string& queryId,
                                            const ExecutionVertex& v) {
    NES_DEBUG("QueryDeployer::getSinks: queryid=" << queryId << " vertex=" << v.id);
    vector<DataSinkPtr> sinks = vector<DataSinkPtr>();
    DataSinkPtr sink = findDataSinkPointer(v.ptr->getRootOperator());

    //FIXME: what about user defined a ZMQ sink?
    if (sink->getType() == ZMQ_SINK) {
        //FIXME: Maybe a better way to do it? perhaps type cast to ZMQSink type and just update the port number
        //create local zmq sink
        const NESTopologyEntryPtr kRootNode = topologyManager->getRootNode();

        sink = createZmqSink(sink->getSchema(), kRootNode->getIp(),
                             assign_port(queryId));
    }
    sinks.emplace_back(sink);
    NES_DEBUG("QueryDeployer::getSinks " << sink->toString());
    return sinks;
}

DataSinkPtr QueryDeployer::findDataSinkPointer(OperatorPtr operatorPtr) {

    if (!operatorPtr->getParent() && operatorPtr->getOperatorType() == SINK_OP) {
        SinkOperator* sinkOperator = dynamic_cast<SinkOperator*>(operatorPtr.get());
        return sinkOperator->getDataSinkPtr();
    } else if (operatorPtr->getParent() != nullptr) {
        return findDataSinkPointer(operatorPtr->getParent());
    } else {
        NES_WARNING("QueryDeployer::findDataSinkPointer Found query graph without a SINK.");
        return nullptr;
    }
}

DataSourcePtr QueryDeployer::findDataSourcePointer(
    OperatorPtr operatorPtr) {
    vector<OperatorPtr> children = operatorPtr->getChildren();
    if (children.empty() && operatorPtr->getOperatorType() == SOURCE_OP) {
        SourceOperator* sourceOperator = dynamic_cast<SourceOperator*>(operatorPtr
                                                                           .get());
        return sourceOperator->getDataSourcePtr();
    } else if (!children.empty()) {
        //FIXME: What if there are more than one source?
        for (OperatorPtr child : children) {
            return findDataSourcePointer(operatorPtr->getParent());
        }
    }
    NES_WARNING("QueryDeployer::findDataSourcePointer Found query graph without a SOURCE.");
    return nullptr;
}

int QueryDeployer::assign_port(const string& queryId) {
    if (this->queryToPort.find(queryId) != this->queryToPort.end()) {
        NES_DEBUG("QueryDeployer::assign_port query found with id=" << queryId << " return port=" << this->queryToPort.at(queryId));
        return this->queryToPort.at(queryId);
    } else {
        // increase max port in map by 1
        const NESTopologyEntryPtr kRootNode = topologyManager->getRootNode();
        uint16_t kFreeZmqPort = kRootNode->getNextFreeReceivePort();
        this->queryToPort.insert({queryId, kFreeZmqPort});
        NES_DEBUG("CoordinatorService::assign_port create a new port for query id=" << queryId << " port=" << kFreeZmqPort);
        return kFreeZmqPort;
    }
}

}// namespace NES

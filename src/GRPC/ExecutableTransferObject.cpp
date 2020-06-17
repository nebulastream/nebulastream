#include <GRPC/ExecutableTransferObject.hpp>
#include <Operators/Impl/WindowOperator.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <Util/Logger.hpp>
#include <boost/algorithm/string/join.hpp>

using std::string;
using std::vector;

namespace NES {
ExecutableTransferObject::ExecutableTransferObject(string queryId,
                                                   SchemaPtr schema,
                                                   vector<DataSourcePtr> sources,
                                                   vector<DataSinkPtr> destinations,
                                                   OperatorPtr operatorTree) {
    this->queryId = std::move(queryId);
    this->schema = schema;
    this->sources = std::move(sources);
    this->destinations = std::move(destinations);
    this->operatorTree = std::move(operatorTree);
}
ExecutableTransferObject::~ExecutableTransferObject() {
    NES_DEBUG("~ExecutableTransferObject()");
}

WindowDefinitionPtr assignWindowHandler(OperatorPtr operator_ptr) {
    for (OperatorPtr c : operator_ptr->getChildren()) {
        if (auto* windowOpt = dynamic_cast<WindowOperator*>(operator_ptr.get())) {
            return windowOpt->getWindowDefinition();
        }
    }
    return nullptr;
};

QueryExecutionPlanPtr ExecutableTransferObject::toQueryExecutionPlan(QueryCompilerPtr queryCompiler) {
    if (!compiled) {
        this->compiled = true;
        NES_INFO("*** Creating QueryExecutionPlan for " << this->queryId);
        QueryExecutionPlanPtr qep = queryCompiler->compile(this->operatorTree);
        qep->setQueryId(queryId);
        //TODO: currently only one input source is supported
        if (!this->sources.empty()) {
            NES_DEBUG("ExecutableTransferObject::toQueryExecutionPlan: add source" << this->sources[0] << " type=" << this->sources[0]->toString());
            qep->addDataSource(this->sources[0]);
        } else {
            NES_ERROR("ExecutableTransferObject::toQueryExecutionPlan The query " << this->queryId << " has no input sources!");
        }

        if (!this->destinations.empty()) {
            qep->addDataSink(this->destinations[0]);
        } else {
            NES_ERROR("The query " << this->queryId << " has no destinations!");
        }

        return qep;
    } else {
        NES_ERROR(this->queryId + " has already been compiled and cannot be recreated!");
    }
}

string& ExecutableTransferObject::getQueryId() {
    return this->queryId;
}

void ExecutableTransferObject::setQueryId(const string& queryId) {
    this->queryId = queryId;
}

SchemaPtr ExecutableTransferObject::getSchema() {
    return this->schema;
}

void ExecutableTransferObject::setSchema(SchemaPtr schema) {
    this->schema = schema;
}

vector<DataSourcePtr>& ExecutableTransferObject::getSources() {
    return this->sources;
}

void ExecutableTransferObject::setSources(const vector<DataSourcePtr>& sources) {
    this->sources = sources;
}

vector<DataSinkPtr>& ExecutableTransferObject::getDestinations() {
    return this->destinations;
}

void ExecutableTransferObject::setDestinations(const vector<DataSinkPtr>& destinations) {
    this->destinations = destinations;
}

OperatorPtr& ExecutableTransferObject::getOperatorTree() {
    return this->operatorTree;
}

void ExecutableTransferObject::setOperatorTree(const OperatorPtr& operatorTree) {
    this->operatorTree = operatorTree;
}

std::string ExecutableTransferObject::toString() const {
    std::vector<std::string> sourcesStringVec;
    if (!sources.empty()) {
        std::transform(std::begin(sources), std::end(sources), std::back_inserter(sourcesStringVec),
                       [](DataSourcePtr d) {
                           return d->toString();
                       });
    }
    std::vector<std::string> destinationsStringVec;
    if (!sources.empty()) {
        std::transform(std::begin(destinations), std::end(destinations), std::back_inserter(destinationsStringVec),
                       [](DataSinkPtr d) {
                           return d->toString();
                       });
    }

    return "ExecutableTransferObject(queryId=(" + queryId + "); schema=(" + schema->toString() + "); sources=("
        + boost::algorithm::join(sourcesStringVec, ";") + "); destinations=("
        + boost::algorithm::join(destinationsStringVec, ",") + "); operatorTree:(" + operatorTree->toString()
        + "); compiled=" + std::to_string(compiled) + ")";
}

}// namespace NES

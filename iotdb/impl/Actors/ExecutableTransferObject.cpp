#include <Actors/ExecutableTransferObject.hpp>
#include <Operators/Impl/WindowOperator.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <Util/Logger.hpp>

using std::string;
using std::vector;

namespace NES {
ExecutableTransferObject::ExecutableTransferObject(string description,
                                                   Schema schema,
                                                   vector<DataSourcePtr> sources,
                                                   vector<DataSinkPtr> destinations,
                                                   OperatorPtr operatorTree) {
  this->_description = std::move(description);
  this->_schema = std::move(schema);
  this->_sources = std::move(sources);
  this->_destinations = std::move(destinations);
  this->_operatorTree = std::move(operatorTree);
}

WindowDefinitionPtr assignWindowHandler(OperatorPtr operator_ptr){
  for(OperatorPtr c: operator_ptr->getChildren()) {
    if (auto *windowOpt = dynamic_cast<WindowOperator *>(operator_ptr.get())) {
      return windowOpt->getWindowDefinition();
    }
  }
  return nullptr;
};

QueryExecutionPlanPtr ExecutableTransferObject::toQueryExecutionPlan() {
  if (!_compiled) {
    this->_compiled = true;
    NES_INFO("*** Creating QueryExecutionPlan for " << this->_description);
    //TODO the query compiler dont has to be initialised per query so we can factore it out.
    auto queryCompiler = createDefaultQueryCompiler();
    QueryExecutionPlanPtr qep = queryCompiler->compile(this->_operatorTree);

    auto window_def = assignWindowHandler(this->_operatorTree);
    if(window_def!= nullptr){
      auto window_handler =  std::make_shared<WindowHandler>(assignWindowHandler(this->_operatorTree));
      qep->addWindow(window_handler);
    }
    //TODO: currently only one input source is supported
    if (!this->_sources.empty()) {
      qep->addDataSource(this->_sources[0]);
    } else {
      NES_ERROR("The query " << this->_description << " has no input sources!")
    }

    if (!this->_destinations.empty()) {
      qep->addDataSink(this->_destinations[0]);
    } else {
      NES_ERROR("The query " << this->_description << " has no destinations!")
    }

    return qep;
  } else {
    NES_ERROR(this->_description + " has already been compiled and cannot be recreated!")
  }
}

string &ExecutableTransferObject::getDescription() {
  return this->_description;
}

void ExecutableTransferObject::setDescription(const string &description) {
  this->_description = description;
}

Schema &ExecutableTransferObject::getSchema() {
  return this->_schema;
}

void ExecutableTransferObject::setSchema(const Schema &schema) {
  this->_schema = schema;
}

vector<DataSourcePtr> &ExecutableTransferObject::getSources() {
  return this->_sources;
}

void ExecutableTransferObject::setSources(const vector<DataSourcePtr> &sources) {
  this->_sources = sources;
}

vector<DataSinkPtr> &ExecutableTransferObject::getDestinations() {
  return this->_destinations;
}

void ExecutableTransferObject::setDestinations(const vector<DataSinkPtr> &destinations) {
  this->_destinations = destinations;
}

OperatorPtr &ExecutableTransferObject::getOperatorTree() {
  return this->_operatorTree;
}

void ExecutableTransferObject::setOperatorTree(const OperatorPtr &operatorTree) {
  this->_operatorTree = operatorTree;
}
}

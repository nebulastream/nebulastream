#include <Actors/ExecutableTransferObject.hpp>

using std::string;
using std::vector;

namespace iotdb {
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

QueryExecutionPlanPtr ExecutableTransferObject::toQueryExecutionPlan() {
  if (!_compiled) {
    this->_compiled = true;
    IOTDB_INFO("*** Creating QueryExecutionPlan for " << this->_description);
    CodeGeneratorPtr code_gen = createCodeGenerator();
    PipelineContextPtr context = createPipelineContext();

    // Parse operators
    this->_operatorTree->produce(code_gen, context, std::cout);
    PipelineStagePtr stage = code_gen->compile(CompilerArgs());

    QueryExecutionPlanPtr qep(new GeneratedQueryExecutionPlan(nullptr, &stage));
    //TODO: currently only one input source is supported
    if (!this->_sources.empty()) {
      qep->addDataSource(this->_sources[0]);
    } else {
      IOTDB_FATAL_ERROR("The query " << this->_description << " has no input sources!")
    }

    if (!this->_destinations.empty()) {
      qep->addDataSink(this->_destinations[0]);
    } else {
      IOTDB_FATAL_ERROR("The query " << this->_description << " has no destinations!")
    }

    return qep;
  } else {
    IOTDB_FATAL_ERROR(this->_description + " has already been compiled and cannot be recreated!")
  }
}

const string &ExecutableTransferObject::getDescription() const {
  return this->_description;
}

void ExecutableTransferObject::setDescription(const string &description) {
  this->_description = description;
}

const Schema &ExecutableTransferObject::getSchema() const {
  return this->_schema;
}

void ExecutableTransferObject::setSchema(const Schema &schema) {
  this->_schema = schema;
}

const vector<DataSourcePtr> &ExecutableTransferObject::getSources() const {
  return this->_sources;
}

void ExecutableTransferObject::setSources(const vector<DataSourcePtr> &sources) {
  this->_sources = sources;
}

const vector<DataSinkPtr> &ExecutableTransferObject::getDestinations() const {
  return this->_destinations;
}

void ExecutableTransferObject::setDestinations(const vector<DataSinkPtr> &destinations) {
  this->_destinations = destinations;
}
const OperatorPtr &ExecutableTransferObject::getOperatorTree() const {
  return this->_operatorTree;
}

void ExecutableTransferObject::setOperatorTree(const OperatorPtr &operatorTree) {
  this->_operatorTree = operatorTree;
}
}

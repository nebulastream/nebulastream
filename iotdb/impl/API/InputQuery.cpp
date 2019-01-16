
#include <API/InputQuery.hpp>
#include <iostream>

namespace iotdb {

InputQuery::~InputQuery() {}

InputQuery InputQuery::create(const Config &config, const Schema &schema, DataSourcePtr source) {
  InputQuery q(config, schema, source);
  return q;
}

void InputQuery::execute() { printQueryPlan(); }

// relational operators
InputQuery &InputQuery::filter(PredicatePtr predicate) {
  OperatorPtr filter = createSelection(predicate);
  filter->childs.push_back(std::move(root));
  root = std::move(filter);
  return *this;
}
InputQuery &InputQuery::groupBy(AttributeFieldPtr field) { return *this; }
InputQuery &InputQuery::orderBy(AttributeFieldPtr field, const std::string &sortedness) { return *this; }
InputQuery &InputQuery::aggregate(Aggregation &&aggregation) { return *this; }
InputQuery &InputQuery::join(const InputQuery &otherQuery, JoinPredicatePtr joinPred) { return *this; }

// streaming operators
InputQuery &InputQuery::window(WindowPtr &&window) { return *this; }
InputQuery &InputQuery::keyBy(AttributeFieldPtr field) { return *this; }
InputQuery &InputQuery::map(Mapper &&mapper) { return *this; }

// output operators
InputQuery &InputQuery::write(std::string file_name) { return *this; }
InputQuery &InputQuery::print() { return *this; }

// helper operators
InputQuery &InputQuery::printQueryPlan() {
  std::cout << "Query Plan: " << std::endl;
  printQueryPlan(root,0);
  return *this;
}

InputQuery::InputQuery(const Config &_config, const Schema &_schema, DataSourcePtr _source)
    : config(_config),
      schema(_schema),
      source(_source),
      root(createScan(source)) {}

InputQuery::InputQuery(InputQuery &query)
  : config(query.config),
    schema(query.schema),
    root(std::move(query.root)) {}

void InputQuery::printQueryPlan(const OperatorPtr &curr, int depth) {
  if (curr) {
    std::cout << std::string(depth*2,' ') << curr->toString() << std::endl;
    for(const auto& op : curr->childs){
      printQueryPlan(op, depth+1);
    }
  }
}

}

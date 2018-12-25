
#include <API/Query.hpp>
#include <iostream>

namespace iotdb {

Query::~Query() {}

Query Query::create(const Config &config, const Schema &schema, DataSourcePtr source) {
  Query q(config, schema, source);
  return q;
}

void Query::execute() { printQueryPlan(); }

// relational operators
Query &Query::filter(PredicatePtr predicate) {
  OperatorPtr filter = createSelection(predicate);
  filter->childs.push_back(std::move(root));
  root = std::move(filter);
  return *this;
}
Query &Query::groupBy(AttributeFieldPtr field) { return *this; }
Query &Query::orderBy(AttributeFieldPtr field, const std::string &sortedness) { return *this; }
Query &Query::aggregate(Aggregation &&aggregation) { return *this; }
Query &Query::join(const Query &otherQuery, JoinPredicatePtr joinPred) { return *this; }

// streaming operators
Query &Query::window(WindowPtr &&window) { return *this; }
Query &Query::keyBy(AttributeFieldPtr field) { return *this; }
Query &Query::map(Mapper &&mapper) { return *this; }

// output operators
Query &Query::write(std::string file_name) { return *this; }
Query &Query::print() { return *this; }

// helper operators
Query &Query::printQueryPlan() {
  std::cout << "Query Plan: " << std::endl;
  printQueryPlan(root,0);
  return *this;
}

Query::Query(const Config &_config, const Schema &_schema, DataSourcePtr _source)
    : config(_config),
      schema(_schema),
      source(_source),
      root(createScan(source)) {}

Query::Query(Query &query)
  : config(query.config),
    schema(query.schema),
    root(std::move(query.root)) {}

void Query::printQueryPlan(const OperatorPtr &curr, int depth) {
  if (curr) {
    std::cout << std::string(depth*2,' ') << curr->toString() << std::endl;
    for(const auto& op : curr->childs){
      printQueryPlan(op, depth+1);
    }
  }
}

}

#ifndef API_INPUT_QUERY_H
#define API_INPUT_QUERY_H

#include <string>
#include <iostream>
#include <API/Schema.hpp>
#include <API/Config.hpp>
#include <API/ParameterTypes.hpp>
#include <Runtime/DataSource.hpp>

namespace iotdb {

class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

/** \brief the central abstraction for the user to define queries */
class InputQuery {
public:
  static InputQuery create(const Config& config, const DataSourcePtr& source);
  InputQuery(const InputQuery& );
  ~InputQuery();

  void execute();

  // relational operators
  InputQuery &filter(const PredicatePtr& predicate);
  InputQuery &groupBy(const Attributes& grouping_fields, const AggregationPtr& aggr_spec);
  InputQuery &orderBy(const Sort& fields);
  InputQuery &join(const InputQuery& sub_query, const JoinPredicatePtr& joinPred);

  // streaming operators
  InputQuery &window(const WindowPtr& window);
  InputQuery &keyBy(const Attributes& fields);
  InputQuery &map(const MapperPtr& mapper);


  // output operators
  InputQuery &writeToFile(const std::string& file_name);
  InputQuery &print(std::ostream& = std::cout);

  // helper operators
  InputQuery &printInputQueryPlan();
private:
  InputQuery(const Config& config, const DataSourcePtr& source);
  Config config;
  DataSourcePtr source;
  OperatorPtr root;
  void printInputQueryPlan(const OperatorPtr& curr, int depth);
};

/* this function **executes** the code provided by the user and returns an InputQuery Object */
const InputQuery createQueryFromCodeString(const std::string&);
typedef std::shared_ptr<InputQuery> InputQueryPtr;

}

#endif

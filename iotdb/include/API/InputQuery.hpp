#ifndef API_INPUT_QUERY_H
#define API_INPUT_QUERY_H

#include <API/Config.hpp>
#include <API/ParameterTypes.hpp>
#include <API/Schema.hpp>
#include <Runtime/DataSource.hpp>
#include <Runtime/Window.hpp>
#include <iostream>
#include <string>

namespace iotdb {

class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

/** \brief the central abstraction for the user to define queries */
class InputQuery {
  public:
    static InputQuery create(const DataSourcePtr& source);
    InputQuery& operator=(const InputQuery& query);
    InputQuery(const InputQuery&);
    ~InputQuery();

    // relational operators
    InputQuery& filter(const PredicatePtr& predicate);
    InputQuery& filter(Predicate predicate);
    InputQuery& groupBy(const Attributes& grouping_fields, const AggregationPtr& aggr_spec);
    InputQuery& orderBy(const Sort& fields);
    InputQuery& join(const InputQuery& sub_query, const JoinPredicatePtr& joinPred);

    // streaming operators
    InputQuery& window(const WindowPtr& window);
    InputQuery& keyBy(const Attributes& fields);
    InputQuery& map(const MapperPtr& mapper);

    // output operators
    InputQuery& writeToFile(const std::string& file_name);
    InputQuery& print(std::ostream& = std::cout);

    // helper operators
    DataSourcePtr getSource() { return source; };
    OperatorPtr getRoot() const { return root; };

private:
    InputQuery(const DataSourcePtr& source);
    DataSourcePtr source;
    OperatorPtr root;
};

/* this function **executes** the code provided by the user and returns an InputQuery Object */
const InputQuery createQueryFromCodeString(const std::string&);
typedef std::shared_ptr<InputQuery> InputQueryPtr;

} // namespace iotdb

#endif

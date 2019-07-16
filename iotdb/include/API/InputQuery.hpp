#ifndef API_INPUT_QUERY_H
#define API_INPUT_QUERY_H

#include <API/Config.hpp>
#include <API/ParameterTypes.hpp>
#include <API/Schema.hpp>
#include <Runtime/DataSource.hpp>
#include <Runtime/Window.hpp>
#include <iostream>
#include <string>
#include "UserAPIExpression.hpp"

namespace iotdb {

class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

/** \brief the central abstraction for the user to define queries */
class InputQuery {
  public:
    static InputQuery from(const std::string sourceStream);
    InputQuery& operator=(const InputQuery& query);
    InputQuery(const InputQuery&);
    ~InputQuery();

    InputQuery& select(const Field field);
    InputQuery& select(const Field field1, const Field field2);
    InputQuery& select(const Field field1, const Field field2, const Field field3);

    InputQuery& filter(const Predicate predicate);
    InputQuery& map(const MapperPtr& mapper);
    InputQuery& combine(const InputQuery& sub_query);
    InputQuery& join(const InputQuery& sub_query, const JoinPredicatePtr& joinPred);
    InputQuery& extractTimestamp(const Field field);

    InputQuery& window(const WindowPtr& window);



    // output operators
    InputQuery& to(const std::string& resultStream);
    InputQuery& writeToFile(const std::string& file_name);
    InputQuery& print(std::ostream& = std::cout);

    // helper operators
    OperatorPtr getRoot() const { return root; };

private:
    InputQuery(const std::string source_stream);
    OperatorPtr root;
    std::string source_stream;
};

/* this function **executes** the code provided by the user and returns an InputQuery Object */
const InputQuery createQueryFromCodeString(const std::string&);
typedef std::shared_ptr<InputQuery> InputQueryPtr;

} // namespace iotdb

#endif

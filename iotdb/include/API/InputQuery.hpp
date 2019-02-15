#ifndef API_INPUT_QUERY_H
#define API_INPUT_QUERY_H

#include <Operators/Operator.hpp>
#include "Source.hpp"
#include "Schema.hpp"
#include "Window.hpp"
#include "Aggregation.hpp"
#include "JoinPredicate.hpp"
#include "Mapper.hpp"
#include <string>
#include "Config.hpp"


namespace iotdb {

//class Config;
//class Schema;

//class Predicate;
//typedef std::shared_ptr<Predicate> PredicatePtr;
//typedef Predicate&& PredicatePtr;

//class JoinPredicate;
//typedef std::shared_ptr<JoinPredicate> JoinPredicatePtr;

//class Aggregation;

//class Window;
//typedef std::shared_ptr<Window> WindowPtr;

//class Mapper;
class InputQuery;
typedef std::shared_ptr<InputQuery> InputQueryPtr;

/** \brief the central abstraction for the user to define queries */
class InputQuery {
public:
  ~InputQuery();
  static InputQueryPtr create(Config& config, Schema& schema, Source& source);

  void execute();

  // relational operators
  InputQuery &filter(Predicate&& predicate);
  InputQuery &groupBy(std::string field);
  InputQuery &orderBy(std::string& field, std::string& sortedness);
  InputQuery &aggregate(Aggregation &&aggregation);
  InputQuery &join(Operator* op, JoinPredicate&& joinPred);

  // streaming operators
  InputQuery &window(Window &&window);
  InputQuery &keyBy(std::string& fieldId);
  InputQuery &map(Mapper&& mapper);

  // output operators
  InputQuery &write(std::string file_name);
  InputQuery &print();

  // helper operators
  InputQuery &printQueryPlan();

  InputQuery &input(InputType type, std::string path);
  InputQuery(Config& config, Schema& schema, Source& source);

private:
//  InputQuery(InputQuery& );
  Config& config;
  Schema& schema;
  Source& source;
  Operator* root;
  Operator* current;
  void printInputQueryPlan(Operator* curr, int depth);
  InputQuery &printInputQueryPlan();

};

}

#endif

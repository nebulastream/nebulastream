#ifndef API_INPUT_QUERY_H
#define API_INPUT_QUERY_H

#include <Operators/Operator.hpp>
#include <API/Source.hpp>
#include <API/Schema.hpp>
#include "Window.hpp"
#include "Aggregation.hpp"
#include "JoinPredicate.hpp"
#include "Mapper.hpp"
#include <string>
#include <API/Config.hpp>


namespace iotdb {

//class Config;
//class Schema;

class Predicate;
typedef std::shared_ptr<Predicate> PredicatePtr;
//typedef Predicate&& PredicatePtr;

//class JoinPredicate;
//typedef std::shared_ptr<JoinPredicate> JoinPredicatePtr;

//class Aggregation;

//class Window;
//typedef std::shared_ptr<Window> WindowPtr;

//class Mapper;

enum SortOrder{ASCENDING,DESCENDING};

struct SortAttr{
    AttributeFieldPtr field;
    SortOrder order;
};

class Sort{
  Sort(AttributeFieldPtr field1);
  Sort(AttributeFieldPtr field1,AttributeFieldPtr field2);
  Sort(AttributeFieldPtr field1,AttributeFieldPtr field2, AttributeFieldPtr field3);
  std::vector<SortAttr> param;
};

/** \brief the central abstraction for the user to define queries */
class InputQuery {
public:
  ~InputQuery();
  static InputQuery create(Config& config, Schema& schema, Source& source);
  InputQuery(const InputQuery& );
  void execute();

  // relational operators
  InputQuery &filter(PredicatePtr predicate);
//  InputQuery &groupBy(const AttributeFieldPtr& field);
//  InputQuery &groupBy(const VecAttributeFieldPtr& field);

//  InputQuery &orderBy(const Sort& field);

//  InputQuery &aggregate(Aggregation &&aggregation);
//  InputQuery &join(InputQuery& sub_query, JoinPredicate&& joinPred);

//  // streaming operators
//  InputQuery &window(Window &&window);
//  InputQuery &keyBy(const AttributeFieldPtr& field);
//  InputQuery &keyBy(const VecAttributeFieldPtr& field);
//  InputQuery &map(Mapper&& mapper);

  // output operators
  //InputQuery &write(const std::string& file_name);
  //InputQuery &print();

  // helper operators
  //InputQuery &printQueryPlan();

  //InputQuery &input(InputType type, std::string path);
  InputQuery &printInputQueryPlan();
private:
  InputQuery(Config& config, Schema& schema, Source& source);
  Config config;
  Schema schema;
  Source& source;
  OperatorPtr root;
  void printInputQueryPlan(Operator* curr, int depth);

};

/* this function **executes** the code provided by the user and returns an InputQuery Object */
InputQuery createQueryFromCodeString(const std::string&);

}
#endif

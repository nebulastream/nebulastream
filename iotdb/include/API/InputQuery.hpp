#ifndef API_INPUT_QUERY_H
#define API_INPUT_QUERY_H

#include <string>
#include <iostream>
#include <API/Source.hpp>
#include <API/Schema.hpp>
#include <API/Config.hpp>
#include <API/ParameterTypes.hpp>
#include <Runtime/DataSource.hpp>


//#include "Window.hpp"
//#include "Aggregation.hpp"
//#include "JoinPredicate.hpp"
//#include "Mapper.hpp"

namespace iotdb {

class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

//class Predicate;
//typedef std::shared_ptr<Predicate> PredicatePtr;

//class JoinPredicate;
//typedef std::shared_ptr<JoinPredicate> JoinPredicatePtr;

//class Aggregation;
//typedef std::shared_ptr<Aggregation> AggregationPtr;

//class Window;
//typedef std::shared_ptr<Window> WindowPtr;

//class Mapper;
//typedef std::shared_ptr<Mapper> MapperPtr;

//struct Attributes{
//  Attributes(AttributeFieldPtr field1);
//  Attributes(AttributeFieldPtr field1,AttributeFieldPtr field2);
//  Attributes(AttributeFieldPtr field1,AttributeFieldPtr field2, AttributeFieldPtr field3);
//  /** \todo add more constructor cases for more fields */
//  std::vector<AttributeFieldPtr> attrs;
//};

//enum SortOrder{ASCENDING,DESCENDING};

//struct SortAttr{
//    AttributeFieldPtr field;
//    SortOrder order;
//};

//struct Sort{
//  Sort(SortAttr field1);
//  Sort(SortAttr field1,SortAttr field2);
//  Sort(SortAttr field1,SortAttr field2, SortAttr field3);
//  /** \todo add more constructor cases for more fields */
//  std::vector<SortAttr> param;
//};

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

}
#endif

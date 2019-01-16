
#pragma once
#include <Operators/Operator.hpp>
#include <Runtime/DataSource.hpp>
#include <Core/DataTypes.hpp>
#include <string>

namespace iotdb {

class Config;
class Schema;

class Predicate;
typedef std::shared_ptr<Predicate> PredicatePtr;

class JoinPredicate;
typedef std::shared_ptr<JoinPredicate> JoinPredicatePtr;

class Aggregation;

class Window;
typedef std::shared_ptr<Window> WindowPtr;

class Mapper;

/** \brief the central abstraction for the user to define queries */
class InputQuery {
public:
  ~InputQuery();
  static InputQuery create(const Config &config, const Schema &schema, DataSourcePtr source);

  void execute();

  // relational operators
  InputQuery &filter(PredicatePtr predicate);
  InputQuery &groupBy(AttributeFieldPtr field);
  InputQuery &orderBy(AttributeFieldPtr field, const std::string &sortedness);
  InputQuery &aggregate(Aggregation &&aggregation);
  InputQuery &join(const InputQuery &otherQuery, JoinPredicatePtr joinPred);

  // streaming operators
  InputQuery &window(WindowPtr &&window);
  InputQuery &keyBy(AttributeFieldPtr field);
  InputQuery &map(Mapper &&mapper);

  // output operators
  InputQuery &write(std::string file_name);
  InputQuery &print();

  // helper operators
  InputQuery &printQueryPlan();

private:
  InputQuery(const Config &config, const Schema &schema, DataSourcePtr source);
  InputQuery(InputQuery &);
  const Config &config;
  const Schema &schema;
  DataSourcePtr source;
  OperatorPtr root;
  void printQueryPlan(const OperatorPtr &curr, int depth);
};
}


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
class Query {
public:
  ~Query();
  static Query create(const Config &config, const Schema &schema, DataSourcePtr source);

  void execute();

  // relational operators
  Query &filter(PredicatePtr predicate);
  Query &groupBy(AttributeFieldPtr field);
  Query &orderBy(AttributeFieldPtr field, const std::string &sortedness);
  Query &aggregate(Aggregation &&aggregation);
  Query &join(const Query &otherQuery, JoinPredicatePtr joinPred);

  // streaming operators
  Query &window(WindowPtr &&window);
  Query &keyBy(AttributeFieldPtr field);
  Query &map(Mapper &&mapper);

  // output operators
  Query &write(std::string file_name);
  Query &print();

  // helper operators
  Query &printQueryPlan();

private:
  Query(const Config &config, const Schema &schema, DataSourcePtr source);
  Query(Query &);
  const Config &config;
  const Schema &schema;
  DataSourcePtr source;
  OperatorPtr root;
  void printQueryPlan(const OperatorPtr &curr, int depth);
};
}

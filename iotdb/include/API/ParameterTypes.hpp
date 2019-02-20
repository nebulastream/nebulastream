
#include <memory>
#include <vector>

namespace iotdb{

class AttributeField;
typedef std::shared_ptr<AttributeField> AttributeFieldPtr;

class Predicate;
typedef std::shared_ptr<Predicate> PredicatePtr;

class JoinPredicate;
typedef std::shared_ptr<JoinPredicate> JoinPredicatePtr;

class Aggregation;
typedef std::shared_ptr<Aggregation> AggregationPtr;

class Window;
typedef std::shared_ptr<Window> WindowPtr;

class Mapper;
typedef std::shared_ptr<Mapper> MapperPtr;

struct Attributes{
  Attributes(AttributeFieldPtr field1);
  Attributes(AttributeFieldPtr field1,AttributeFieldPtr field2);
  Attributes(AttributeFieldPtr field1,AttributeFieldPtr field2, AttributeFieldPtr field3);
  /** \todo add more constructor cases for more fields */
  std::vector<AttributeFieldPtr> attrs;
};

enum SortOrder{ASCENDING,DESCENDING};

struct SortAttr{
    AttributeFieldPtr field;
    SortOrder order;
};

struct Sort{
  Sort(SortAttr field1);
  Sort(SortAttr field1,SortAttr field2);
  Sort(SortAttr field1,SortAttr field2, SortAttr field3);
  /** \todo add more constructor cases for more fields */
  std::vector<SortAttr> param;
};

}


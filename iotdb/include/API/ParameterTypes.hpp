
#pragma once

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

struct AggregationSpec{
 Aggregation& grouping_fields;
 AggregationPtr aggr_spec;
 AggregationSpec& operator=(const AggregationSpec&);
};

class WindowFunction;
typedef std::shared_ptr<WindowFunction> WindowFunctionPtr;

class TimeMeasure;
typedef std::shared_ptr<TimeMeasure> TimeMeasurePtr;

class Window;
typedef std::shared_ptr<Window> WindowPtr;

const WindowPtr createTumblingWindow(const TimeMeasure&, const WindowFunction&, uint32_t unit);
const WindowPtr createSlidingWindow(const TimeMeasure&, const WindowFunction&, uint32_t slide, uint32_t step);
const WindowPtr createSessionWindow(const TimeMeasure&, const WindowFunction&, uint32_t session_gap);

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

class DataSource;
typedef std::shared_ptr<DataSource> DataSourcePtr;

class DataSink;
typedef std::shared_ptr<DataSink> DataSinkPtr;

const DataSourcePtr copy(const DataSourcePtr&);
const DataSinkPtr copy(const DataSinkPtr&);
const PredicatePtr copy(const PredicatePtr&);
const Sort copy(const Sort&);
const WindowPtr copy(const WindowPtr&);
const Attributes copy(const Attributes&);
const MapperPtr copy(const MapperPtr&);
const AggregationSpec copy(const AggregationSpec&);
const JoinPredicatePtr copy(const JoinPredicatePtr&);

const std::string toString(const DataSourcePtr&);
const std::string toString(const DataSinkPtr&);
const std::string toString(const PredicatePtr&);
const std::string toString(const Sort&);
const std::string toString(const WindowPtr&);
const std::string toString(const Attributes&);
const std::string toString(const MapperPtr&);
const std::string toString(const AggregationSpec&);
const std::string toString(const JoinPredicatePtr&);




}


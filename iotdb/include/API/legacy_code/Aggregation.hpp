#ifndef API_AGGREGATION_H
#define API_AGGREGATION_H

#include "Field.hpp"
#include "../CodeGen/CodeGen.hpp"
#include "../Operators/Operator.hpp"

namespace iotdb {

class Aggregation {
public:
  Aggregation() {}
  Aggregation(std::string fieldId) : fieldId(fieldId) {}

  virtual std::string to_string() { return "Aggregation"; }

protected:
  size_t pipeline;
  std::string fieldId;
};

class Sum : public Aggregation {
public:
  Sum(std::string fieldId) : Aggregation(fieldId) {}

  std::string to_string() { return "Sum(" + fieldId + ")"; };
};

class Count : public Aggregation {
public:
  Count() : Aggregation() {}

  std::string to_string() { return "Count(" + fieldId + ")"; };
};

class Min : public Aggregation {
public:
  Min(std::string fieldId) : Aggregation(fieldId) {}

  std::string to_string() { return "Min(" + fieldId + ")"; };
};

class Max : public Aggregation {
public:
  Max(std::string fieldId) : Aggregation(fieldId) {}

  std::string to_string() { return "Max(" + fieldId + ")"; };
};

class Avg : public Aggregation {
public:
  Avg(std::string fieldId) : Aggregation(fieldId) {}

  std::string to_string() { return "Avg(" + fieldId + ")"; };
};
}

#endif // API_AGGREGATION_H

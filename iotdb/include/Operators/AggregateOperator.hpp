#ifndef OPERATOR_AGGREGATE_OPERATOR_H
#define OPERATOR_AGGREGATE_OPERATOR_H

#include <API/Aggregation.hpp>
#include "Operator.hpp"

namespace iotdb{
class AggregateOperator : public Operator {
public:
  AggregateOperator(Aggregation& aggregation, Operator* input): aggregation(aggregation), input(input){};
  std::string to_string(){ return "Aggregate " + aggregation.to_string(); };

private:
  Aggregation &aggregation;
  Operator *input;
};
}
#endif // OPERATOR_AGGREGATE_OPERATOR_H

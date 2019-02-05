#ifndef OPERATOR_FILTER_OPERATOR_H
#define OPERATOR_FILTER_OPERATOR_H

#include <API/Predicate.hpp>
#include "Operator.hpp"

namespace iotdb{
class FilterOperator : public Operator {
public:
  FilterOperator(Predicate &predicate, Operator *input): predicate(predicate), input(input){};
  std::string to_string(){ return "Select " + predicate.to_string(); };

private:
  Predicate &predicate;
  Operator *input;
};
}
#endif // OPERATOR_FILTER_OPERATOR_H

#ifndef OPERATOR_FILTER_OPERATOR_H
#define OPERATOR_FILTER_OPERATOR_H

#include <API/Predicate.hpp>
#include <API/InputQuery.hpp>
#include <Operators/Operator.hpp>

namespace iotdb{
class FilterOperator : public Operator {
public:
  FilterOperator(PredicatePtr &predicate, Operator *input): predicate(predicate){};
  std::string to_string(){ return "Select " + predicate->to_string(); };

private:
  PredicatePtr predicate;
};
}
#endif // OPERATOR_FILTER_OPERATOR_H

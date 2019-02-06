#ifndef OPERATOR_GROUPBY_OPERATOR_H
#define OPERATOR_GROUPBY_OPERATOR_H

#include "Operator.hpp"
#include <API/Field.hpp>

namespace iotdb{
class GroupByOperator : public Operator {
public:
  GroupByOperator(Field &field, Operator *input): field(field), input(input){};
  std::string to_string() { return "Group by " + field.name; };

private:
  Field &field;
  Operator *input;
};
}
#endif // OPERATOR_GROUPBY_OPERATOR_H

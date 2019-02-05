#ifndef OPERATOR_ORDERBY_OPERATOR_H
#define OPERATOR_ORDERBY_OPERATOR_H

#include "Operator.hpp"
#include <API/Field.hpp>

namespace iotdb {
class OrderByOperator : public Operator {
public:
  OrderByOperator(Field &field, Operator *input): field(field), input(input){};
  std::string to_string(){ return "Order by " + field.name; };

private:
  Field &field;
  Operator *input;
};
}

#endif // OPERATOR_ORDERBY_OPERATOR_H

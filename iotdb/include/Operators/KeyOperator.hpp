#ifndef OPERATOR_KEY_OPERATOR_H
#define OPERATOR_KEY_OPERATOR_H

#include "Operator.hpp"
#include <API/Field.hpp>

namespace iotdb{
class KeyOperator : public Operator {
public:
  KeyOperator(Field &field, Operator *input): field(field), input(input){};
  std::string to_string(){ return "Key by " + field.name; };

private:
  Field &field;
  Operator *input;
};
}

#endif // OPERATOR_KEY_OPERATOR_H

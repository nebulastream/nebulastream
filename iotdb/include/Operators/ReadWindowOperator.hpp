#ifndef OPERATOR_READ_WINDOW_OPERATOR_H
#define OPERATOR_READ_WINDOW_OPERATOR_H

#include "Operator.hpp"
#include <API/Schema.hpp>
namespace iotdb {
class ReadWindowOperator : public Operator {
public:
  ReadWindowOperator(Schema &schema, Operator *input): schema(schema), input(input){};
  std::string to_string() { return "Read Window"; };

private:
  Schema &schema;
  Operator *input;
};
}

#endif // OPERATOR_READ_OPERATOR_H

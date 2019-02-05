#ifndef OPERATOR_READ_OPERATOR_H
#define OPERATOR_READ_OPERATOR_H

#include "Operator.hpp"
#include <API/Schema.hpp>

namespace iotdb {
class ReadOperator : public Operator {
public:
  ReadOperator(Schema &schema): schema(schema){};
  std::string to_string(){ return "Read Schema"; };

private:
  Schema &schema;
};
}

#endif // OPERATOR_READ_OPERATOR_H

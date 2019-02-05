#ifndef OPERATOR_PRINT_OPERATOR_H
#define OPERATOR_PRINT_OPERATOR_H

#include <cstddef>

#include "Operator.hpp"

namespace iotdb {
class PrintOperator : public Operator {
public:
  PrintOperator(Operator *input): input(input){};
  std::string to_string(){ return "Print"; };

private:
  Operator *input;
};
}

#endif // OPERATOR_PRINT_OPERATOR_H

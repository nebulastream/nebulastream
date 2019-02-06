#ifndef OPERATOR_GENERATE_OPERATOR_H
#define OPERATOR_GENERATE_OPERATOR_H

#include "Operator.hpp"

namespace iotdb{
class GenerateOperator : public Operator {
public:
  GenerateOperator(Operator *input): input(input){};
  std::string to_string(){ return "Generate"; };

private:
  Operator *input;
};
}

#endif // OPERATOR_GENERATE_OPERATOR_H

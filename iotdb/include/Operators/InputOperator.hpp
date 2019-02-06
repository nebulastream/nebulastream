#ifndef OPERATOR_INPUT_OPERATOR_H
#define OPERATOR_INPUT_OPERATOR_H

#include "Operator.hpp"
#include <API/InputTypes.hpp>

namespace iotdb{
class InputOperator : public Operator {
public:
  InputOperator(InputType type, std::string path, Operator *input): type(type), input(input){};
  std::string to_string() {
    auto string_representation = std::string("Input[");
    string_representation += type.to_string() + std::string(", ");
    string_representation += path + std::string("]");
    return string_representation;
  }

  InputType getInputType() { return type; }

  std::string getPath() { return path; }

private:
  InputType type;
  std::string path;
  Operator *input;
};
}

#endif // OPERATOR_INPUT_OPERATOR_H

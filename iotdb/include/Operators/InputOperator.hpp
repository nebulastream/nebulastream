#ifndef OPERATOR_INPUT_OPERATOR_H
#define OPERATOR_INPUT_OPERATOR_H

#include <Operators/Operator.hpp>
#include <API/InputTypes.hpp>

namespace iotdb{
class InputOperator : public Operator {
public:
  InputOperator(InputType type, std::string path) : Operator(), type(type), path(path){};
  std::string to_string() {
    auto string_representation = std::string("Input[");
    string_representation += type.to_string() + std::string(", ");
    string_representation += path + std::string("]");
    return string_representation;
  }

  InputType getInputType() { return type; }

  std::string getPath() { return path; }
  ~InputOperator();
private:
  InputType type;
  std::string path;
};

InputOperator::~InputOperator(){

}

}

#endif // OPERATOR_INPUT_OPERATOR_H

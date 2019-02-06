#ifndef OPERATOR_WRITE_OPERATOR_H
#define OPERATOR_WRITE_OPERATOR_H

#include <cstddef>

#include "Operator.hpp"

namespace iotdb {
class WriteOperator : public Operator {
public:
  WriteOperator(Operator *input, std::string file_name): input(input), file_name(file_name){};
  std::string to_string(){ return "Write File " + file_name; };

private:
  Operator *input;
  std::string file_name;

};
}
#endif // OPERATOR_WRITE_OPERATOR_H

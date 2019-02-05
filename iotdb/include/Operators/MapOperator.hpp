#ifndef OPERATOR_MAP_OPERATOR_H
#define OPERATOR_MAP_OPERATOR_H

#include "API/Mapper.hpp"
#include "Operator.hpp"

namespace iotdb {
class MapOperator : public Operator {
public:
  MapOperator(Mapper &mapper, Operator *input): mapper(mapper), input(input){};
  std::string to_string(){ return "Map"; } ;

private:
  Mapper &mapper;
  Operator *input;
};
}
#endif // OPERATOR_MAP_OPERATOR_H

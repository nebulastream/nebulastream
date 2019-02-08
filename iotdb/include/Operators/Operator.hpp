#ifndef OPERATOR_OPERATOR_H
#define OPERATOR_OPERATOR_H

#include <string>
#include <memory>

namespace iotdb{



class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

class Operator {
public:
  virtual ~Operator();
  const OperatorPtr copy(){return OperatorPtr();}
  Operator *leftChild;
  Operator *rightChild;
  std::string name;
  size_t pipeline;
  size_t cost;

  virtual std::string to_string() { return "Operator"; }
};

}



#endif // OPERATOR_OPERATOR_H

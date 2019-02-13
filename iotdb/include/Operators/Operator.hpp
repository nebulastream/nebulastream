#ifndef OPERATOR_OPERATOR_H
#define OPERATOR_OPERATOR_H

#include <string>


class Operator {
public:
  virtual ~Operator() {}
  Operator *parent;
  Operator *leftChild;
  Operator *rightChild;
  std::string name;
  size_t pipeline;
  size_t cost;

  virtual std::string to_string() { return "Operator"; }
};

#endif // OPERATOR_OPERATOR_H

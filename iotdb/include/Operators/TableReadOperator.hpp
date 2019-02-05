#ifndef OPERATOR_TABLE_READ_OPERATOR_H
#define OPERATOR_TABLE_READ_OPERATOR_H

#include "Operator.hpp"

namespace iotdb {
class TableReadOperator : public Operator {
public:
  TableReadOperator(std::string tableName): tableName(tableName){};
  std::string to_string();

private:
  std::string tableName;
};
}

#endif // OPERATOR_TABLE_READ_OPERATOR_H

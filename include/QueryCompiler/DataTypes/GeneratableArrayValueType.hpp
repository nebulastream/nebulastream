#ifndef NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEARRAYVALUETYPE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEARRAYVALUETYPE_HPP_

#include <DataTypes/ValueTypes/ValueType.hpp>
#include <QueryCompiler/DataTypes/GeneratableValueType.hpp>
#include <string>
#include <vector>
namespace NES {

class GeneratableArrayValueType : public GeneratableValueType {
  public:
    GeneratableArrayValueType(ValueTypePtr valueType, std::vector<std::string> values, bool isString = false);
    CodeExpressionPtr getCodeExpression() override;

  private:
    ValueTypePtr valueType;
    std::vector<std::string> values;
    bool isString;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEBASICVALUETYPE_HPP_

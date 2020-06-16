
#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CONSTANTEXPRESSIONSTATEMENT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CONSTANTEXPRESSIONSTATEMENT_HPP_

#include <QueryCompiler/CCodeGenerator/Statements/ExpressionStatement.hpp>

namespace NES {

class GeneratableValueType;
typedef std::shared_ptr<GeneratableValueType> GeneratableValueTypePtr;

class ConstantExpressionStatement : public ExpressionStatment {
  public:
    virtual StatementType getStamentType() const;

    virtual const CodeExpressionPtr getCode() const;

    virtual const ExpressionStatmentPtr copy() const;

    ConstantExpressionStatement(GeneratableValueTypePtr val);

    virtual ~ConstantExpressionStatement();

  private:
    GeneratableValueTypePtr constantValue;
};

typedef ConstantExpressionStatement Constant;

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CONSTANTEXPRESSIONSTATEMENT_HPP_

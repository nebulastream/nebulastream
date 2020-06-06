#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_VARREFSTATEMENT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_VARREFSTATEMENT_HPP_
#include <QueryCompiler/CCodeGenerator/ExpressionStatement.hpp>
namespace NES {

class VarRefStatement : public ExpressionStatment {
  public:
    VariableDeclarationPtr varDeclaration;

    virtual StatementType getStamentType() const override;

    virtual const CodeExpressionPtr getCode() const override;

    virtual const ExpressionStatmentPtr copy() const override;

    VarRefStatement(const VariableDeclaration& var_decl);

    VarRefStatement(VariableDeclarationPtr varDeclaration);

    ~VarRefStatement();
};

typedef VarRefStatement VarRef;

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_VARREFSTATEMENT_HPP_

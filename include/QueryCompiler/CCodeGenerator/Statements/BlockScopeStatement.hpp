#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_STATEMENTS_BLOCKSCOPESTATEMENT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_STATEMENTS_BLOCKSCOPESTATEMENT_HPP_

#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/CompoundStatement.hpp>
namespace NES{

class BlockScopeStatement;
typedef std::shared_ptr<BlockScopeStatement> BlockScopeStatementPtr;

class BlockScopeStatement : public CompoundStatement {
  public:
    static BlockScopeStatementPtr create();

    const CodeExpressionPtr getCode() const override;

};

}

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_STATEMENTS_BLOCKSCOPESTATEMENT_HPP_

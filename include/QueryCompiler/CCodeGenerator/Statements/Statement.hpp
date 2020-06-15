#pragma once

#include <memory>
#include <string>

#include <DataTypes/DataType.hpp>
#include <DataTypes/ValueTypes/ValueType.hpp>
#include <QueryCompiler/CCodeGenerator/Declarations/VariableDeclaration.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <utility>

namespace NES {

enum StatementType {
    RETURN_STMT,
    IF_STMT,
    IF_ELSE_STMT,
    FOR_LOOP_STMT,
    FUNC_CALL_STMT,
    VAR_REF_STMT,
    VAR_DEC_STMT,
    CONSTANT_VALUE_EXPR_STMT,
    BINARY_OP_STMT,
    UNARY_OP_STMT,
    COMPOUND_STMT
};

enum BracketMode { NO_BRACKETS,
                   BRACKETS };

class Statement;
typedef std::shared_ptr<Statement> StatementPtr;

class Statement {
  public:
    virtual StatementType getStamentType() const = 0;
    virtual const CodeExpressionPtr getCode() const = 0;
    virtual const StatementPtr createCopy() const = 0;
    /** \brief virtual copy constructor */
    virtual ~Statement();
};

struct AssignmentStatment {
    VariableDeclaration lhs_tuple_var;
    VariableDeclaration lhs_field_var;
    VariableDeclaration lhs_index_var;
    VariableDeclaration rhs_tuple_var;
    VariableDeclaration rhs_field_var;
    VariableDeclaration rhs_index_var;
};

}// namespace NES

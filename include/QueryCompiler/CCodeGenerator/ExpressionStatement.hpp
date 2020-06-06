#pragma once
#include <QueryCompiler/CCodeGenerator/Statement.hpp>

namespace NES {

class ExpressionStatment;
typedef std::shared_ptr<ExpressionStatment> ExpressionStatmentPtr;

class BinaryOperatorStatement;

class ExpressionStatment : public Statement {
  public:
    virtual StatementType getStamentType() const = 0;
    virtual const CodeExpressionPtr getCode() const = 0;
    /** \brief virtual copy constructor */
    virtual const ExpressionStatmentPtr copy() const = 0;
    /** \brief virtual copy constructor of base class
     *  \todo create one unified version, having this twice is problematic
    */
    const StatementPtr createCopy() const override final;
    //  UnaryOperatorStatement operator sizeof(const ExpressionStatment &ref){
    //  return UnaryOperatorStatement(ref, SIZE_OF_TYPE_OP);
    //  }

    BinaryOperatorStatement assign(const ExpressionStatment& ref);
    BinaryOperatorStatement accessPtr(const ExpressionStatment& ref);
    BinaryOperatorStatement accessRef(const ExpressionStatment& ref);

    BinaryOperatorStatement operator[](const ExpressionStatment& ref);

    // UnaryOperatorStatement operator ++(const ExpressionStatment &ref){
    // return UnaryOperatorStatement(ref, POSTFIX_INCREMENT_OP);
    //}
    // UnaryOperatorStatement operator --(const ExpressionStatment &ref){
    // return UnaryOperatorStatement(ref, POSTFIX_DECREMENT_OP);
    //}

    virtual ~ExpressionStatment();
};

}
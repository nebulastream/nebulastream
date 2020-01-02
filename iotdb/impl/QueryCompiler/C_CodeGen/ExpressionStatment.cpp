
#include <QueryCompiler/C_CodeGen/BinaryOperatorStatement.hpp>
#include <QueryCompiler/C_CodeGen/Statement.hpp>

namespace iotdb {

BinaryOperatorStatement ExpressionStatment::operator[](const ExpressionStatment& ref)
{
    return BinaryOperatorStatement(*this, ARRAY_REFERENCE_OP, ref);
}

BinaryOperatorStatement ExpressionStatment::accessPtr(const ExpressionStatment& ref)
{
    return BinaryOperatorStatement(*this, MEMBER_SELECT_POINTER_OP, ref);
}

BinaryOperatorStatement ExpressionStatment::accessRef(const ExpressionStatment& ref)
{
    return BinaryOperatorStatement(*this, MEMBER_SELECT_REFERENCE_OP, ref);
}

BinaryOperatorStatement ExpressionStatment::assign(const ExpressionStatment& ref)
{
    return BinaryOperatorStatement(*this, ASSIGNMENT_OP, ref);
}

} // namespace iotdb

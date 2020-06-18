#ifndef NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEDATATYPE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEDATATYPE_HPP_

#include <memory>

namespace NES {

class CodeExpression;
typedef std::shared_ptr<CodeExpression> CodeExpressionPtr;

class GeneratableDataType;
typedef std::shared_ptr<GeneratableDataType> GeneratableDataTypePtr;

class AssignmentStatment;
class Statement;
typedef std::shared_ptr<Statement> StatementPtr;

/**
 * @brief A generatable data type,
 * generate codes with regards to a particular data type.
 */
class GeneratableDataType {
  public:
    GeneratableDataType() = default;
    virtual const CodeExpressionPtr getCode() const = 0;
    virtual const CodeExpressionPtr getTypeDefinitionCode() const = 0;
    virtual CodeExpressionPtr  getDeclCode(std::string identifier) = 0;
    virtual StatementPtr getStmtCopyAssignment(const AssignmentStatment& aParam) ;
};
}

#endif//NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEDATATYPE_HPP_

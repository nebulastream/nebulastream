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

    /**
    * @brief Generates the code for the native type.
    * For instance int8_t, or uint32_t for BasicTypes or uint32_t[15] for an ArrayType.
    * @return CodeExpressionPtr
    */
    virtual const CodeExpressionPtr getCode() const = 0;

    /**
     * @brief Generated code for a type definition. This is mainly crucial for structures.
     * @return CodeExpressionPtr
     */
    virtual const CodeExpressionPtr getTypeDefinitionCode() const = 0;

    /**
    * @brief Generates the code for a type declaration with a specific identifier.
    * For instance "int8_t test", or "uint32_t test" for BasicTypes or "uint32_t test[15]" for an ArrayType.
    * @return CodeExpressionPtr
    */
    virtual CodeExpressionPtr getDeclarationCode(std::string identifier) = 0;

    /**
    * @brief Create copy assignment between two types.
    * @deprecated this will move to an own copy statement in the future.
    * @return CodeExpressionPtr
    */
    virtual StatementPtr getStmtCopyAssignment(const AssignmentStatment& aParam);
};
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEDATATYPE_HPP_

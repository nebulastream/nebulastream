#ifndef NES_INCLUDE_QUERYCOMPILER_DATATYPES_ANONYMOUSUSERDEFINEDDATATYPE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_DATATYPES_ANONYMOUSUSERDEFINEDDATATYPE_HPP_

#include <memory>
#include <string>

#include <QueryCompiler/CCodeGenerator/Declarations/Declaration.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <utility>

namespace NES {

/**
 * @brief A generatable data type that generates code for anonymous user define types.
 * This is usually used to generate code for runtime types, which are not covered by the nes type system.
 */
class AnonymousUserDefinedDataType : public GeneratableDataType {
  public:
    AnonymousUserDefinedDataType(const std::string name);

    /**
     * @brief Generated code for a type definition. This is mainly crucial for structures.
     * @return CodeExpressionPtr
     */
    const CodeExpressionPtr getTypeDefinitionCode() const override;

    /**
    * @brief Generates the code for a type declaration with a specific identifier.
    * For instance "int8_t test", or "uint32_t test" for BasicTypes or "uint32_t test[15]" for an ArrayType.
    * @return CodeExpressionPtr
    */
    CodeExpressionPtr getDeclarationCode(std::string identifier) override;

    /**
    * @brief Generates the code for the native type.
    * For instance int8_t, or uint32_t for BasicTypes or uint32_t[15] for an ArrayType.
    * @return CodeExpressionPtr
    */
    const CodeExpressionPtr getCode() const override;
    ~AnonymousUserDefinedDataType();

  private:
    const std::string name;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_DATATYPES_ANONYMOUSUSERDEFINEDDATATYPE_HPP_

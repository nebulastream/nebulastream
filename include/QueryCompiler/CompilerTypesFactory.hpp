#ifndef NES_INCLUDE_QUERYCOMPILER_COMPILERTYPESFACTORY_HPP_
#define NES_INCLUDE_QUERYCOMPILER_COMPILERTYPESFACTORY_HPP_
#include <memory>
namespace NES {

class GeneratableDataType;
typedef std::shared_ptr<GeneratableDataType> GeneratableDataTypePtr;

class GeneratableValueType;
typedef std::shared_ptr<GeneratableValueType> GeneratableValueTypePtr;

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class ValueType;
typedef std::shared_ptr<ValueType> ValueTypePtr;

class StructDeclaration;

/**
 * @brief The compiler type factory creates generatable data types, which are required during query compilation.
 */
class CompilerTypesFactory {
  public:

    /**
     * @brief Create a annonymus data type, which is used to represent types of the runtime system that are not covered by the nes type system.
     * @param type
     * @return GeneratableDataTypePtr
     */
    GeneratableDataTypePtr createAnonymusDataType(std::string type);

    /**
    * @brief Create a generatable data type, which corresponds to a particular nes data type.
    * @param type nes data type
    * @return GeneratableDataTypePtr
    */
    GeneratableDataTypePtr createDataType(DataTypePtr type);

    /**
    * @brief Create a user defined type, which corresponds to a struct declaration. This is used to represent the input and output of a pipeline.
    * @param structDeclaration the struct declaration.
    * @return GeneratableDataTypePtr
    */
    GeneratableDataTypePtr createUserDefinedType(StructDeclaration structDeclaration);

    /**
     * @brief Create a reference from a GeneratableDataType
     * @param type GeneratableDataTypePtr
     * @return GeneratableDataTypePtr
     */
    GeneratableDataTypePtr createReference(GeneratableDataTypePtr type);

    /**
    * @brief Create a pointer from a GeneratableDataType
    * @param type GeneratableDataTypePtr
    * @return GeneratableDataTypePtr
    */
    GeneratableDataTypePtr createPointer(GeneratableDataTypePtr type);

    /**
    * @brief Create a value type from a GeneratableDataType
    * @param type GeneratableDataTypePtr
    * @return GeneratableDataTypePtr
    */
    GeneratableValueTypePtr createValueType(ValueTypePtr valueType);
};
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_COMPILERTYPESFACTORY_HPP_

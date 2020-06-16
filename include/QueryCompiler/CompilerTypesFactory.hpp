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

class CompilerTypesFactory {
  public:
    GeneratableDataTypePtr createAnonymusDataType(std::string type);
    GeneratableDataTypePtr createDataType(DataTypePtr type);
    GeneratableDataTypePtr createUserDefinedType(StructDeclaration structDeclaration);
    GeneratableDataTypePtr createReference(GeneratableDataTypePtr type);
    GeneratableDataTypePtr createPointer(GeneratableDataTypePtr type);
    GeneratableValueTypePtr createValueType(ValueTypePtr valueType);



};
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_COMPILERTYPESFACTORY_HPP_

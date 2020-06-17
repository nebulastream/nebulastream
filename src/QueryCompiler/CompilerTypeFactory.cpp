
#include <DataTypes/ValueTypes/BasicValue.hpp>
#include <DataTypes/ValueTypes/ValueType.hpp>
#include <DataTypes/ValueTypes/ArrayValueType.hpp>
#include <DataTypes/ValueTypes/CharValueType.hpp>
#include <DataTypes/Array.hpp>
#include <DataTypes/FixedChar.hpp>
#include <DataTypes/PhysicalTypes/ArrayPhysicalType.hpp>
#include <DataTypes/PhysicalTypes/BasicPhysicalType.hpp>
#include <DataTypes/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <QueryCompiler/DataTypes/GeneratableBasicValueType.hpp>
#include <QueryCompiler/DataTypes/GeneratableArrayValueType.hpp>
#include <DataTypes/DataTypeFactory.hpp>
#include <DataTypes/PhysicalTypes/PhysicalTypeFactory.hpp>
#include <QueryCompiler/CCodeGenerator/Declarations/StructDeclaration.hpp>
#include <QueryCompiler/CompilerTypesFactory.hpp>
#include <QueryCompiler/DataTypes/ArrayGeneratableType.hpp>
#include <QueryCompiler/DataTypes/BasicGeneratableType.hpp>
#include <QueryCompiler/DataTypes/PointerDataType.hpp>
#include <QueryCompiler/DataTypes/UserDefinedDataType.hpp>
#include <QueryCompiler/DataTypes/ReferenceDataType.hpp>
#include <QueryCompiler/DataTypes/AnonymousUserDefinedDataType.hpp>
#include <Util/Logger.hpp>
namespace NES{

GeneratableDataTypePtr CompilerTypesFactory::createDataType(DataTypePtr type) {
    if(type->isArray()){
        auto arrayType = DataType::as<Array>(type);
        auto componentDataType = createDataType(arrayType->getComponent());
        auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(type);
        auto arrayPhysicalType = std::dynamic_pointer_cast<ArrayPhysicalType>(physicalType);
        return std::make_shared<ArrayGeneratableType>(arrayPhysicalType, componentDataType);
    } else if(type->isFixedChar()){
        auto charType = DataType::as<FixedChar>(type);
        auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(type);
        auto arrayPhysicalType = std::dynamic_pointer_cast<ArrayPhysicalType>(physicalType);
        return std::make_shared<ArrayGeneratableType>(arrayPhysicalType, createDataType(DataTypeFactory::createChar()));
    } else {
        auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(type);
        auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
        return std::make_shared<BasicGeneratableType>(basicPhysicalType);
    }
    NES_THROW_RUNTIME_ERROR("CompilerTypesFactory:: Error");
}

GeneratableValueTypePtr CompilerTypesFactory::createValueType(ValueTypePtr valueType) {
    if(valueType->isBasicValue()){
        return std::make_shared<GeneratableBasicValueType>(std::dynamic_pointer_cast<BasicValue>(valueType));
    }else if(valueType->isArrayValue()){
        return std::make_shared<GeneratableArrayValueType>(valueType, std::dynamic_pointer_cast<ArrayValue>(valueType)->getValues());
    }else if(valueType->isCharValue()){
        auto charValue=  std::dynamic_pointer_cast<CharValueType>(valueType);
        return std::make_shared<GeneratableArrayValueType>(valueType,charValue->getValues(), charValue->isString1() );
    }
    NES_THROW_RUNTIME_ERROR("CompilerTypesFactory:: Error");

}

GeneratableDataTypePtr CompilerTypesFactory::createAnonymusDataType(std::string type) {
    return std::make_shared<AnonymousUserDefinedDataType>(type);
}

GeneratableDataTypePtr CompilerTypesFactory::createUserDefinedType(StructDeclaration structDeclaration) {
    return std::make_shared<UserDefinedDataType>(structDeclaration);
}

GeneratableDataTypePtr CompilerTypesFactory::createPointer(GeneratableDataTypePtr type) {
    return std::make_shared<PointerDataType>(type);
}

GeneratableDataTypePtr CompilerTypesFactory::createReference(GeneratableDataTypePtr type) {
    return std::make_shared<ReferenceDataType>(type);
}

}
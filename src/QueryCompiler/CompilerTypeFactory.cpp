
#include <Common/DataTypes/Array.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/FixedChar.hpp>
#include <Common/PhysicalTypes/ArrayPhysicalType.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/ValueTypes/ArrayValue.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
#include <Common/ValueTypes/FixedCharValue.hpp>
#include <QueryCompiler/CCodeGenerator/Declarations/StructDeclaration.hpp>
#include <QueryCompiler/CompilerTypesFactory.hpp>
#include <QueryCompiler/GeneratableTypes/AnonymousUserDefinedDataType.hpp>
#include <QueryCompiler/GeneratableTypes/ArrayGeneratableType.hpp>
#include <QueryCompiler/GeneratableTypes/BasicGeneratableType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableArrayValueType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableBasicValueType.hpp>
#include <QueryCompiler/GeneratableTypes/PointerDataType.hpp>
#include <QueryCompiler/GeneratableTypes/ReferenceDataType.hpp>
#include <QueryCompiler/GeneratableTypes/UserDefinedDataType.hpp>
#include <Util/Logger.hpp>
namespace NES {

GeneratableDataTypePtr CompilerTypesFactory::createDataType(DataTypePtr type) {
    if (type->isArray()) {
        auto arrayType = DataType::as<Array>(type);
        auto componentDataType = createDataType(arrayType->getComponent());
        auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(type);
        auto arrayPhysicalType = std::dynamic_pointer_cast<ArrayPhysicalType>(physicalType);
        return std::make_shared<ArrayGeneratableType>(arrayPhysicalType, componentDataType);
    } else if (type->isFixedChar()) {
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
    if (valueType->isBasicValue()) {
        return std::make_shared<GeneratableBasicValueType>(std::dynamic_pointer_cast<BasicValue>(valueType));
    } else if (valueType->isArrayValue()) {
        return std::make_shared<GeneratableArrayValueType>(valueType, std::dynamic_pointer_cast<ArrayValue>(valueType)->getValues());
    } else if (valueType->isCharValue()) {
        auto charValue = std::dynamic_pointer_cast<FixedCharValue>(valueType);
        return std::make_shared<GeneratableArrayValueType>(valueType, charValue->getValues(), charValue->getIsString());
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

}// namespace NES
/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Common/DataTypes/ArrayType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/FixedChar.hpp>
#include <Common/PhysicalTypes/ArrayPhysicalType.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/ValueTypes/ArrayValue.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/StructDeclaration.hpp>
#include <QueryCompiler/GeneratableTypes/AnonymousUserDefinedDataType.hpp>
#include <QueryCompiler/GeneratableTypes/ArrayGeneratableType.hpp>
#include <QueryCompiler/GeneratableTypes/BasicGeneratableType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableArrayValueType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableBasicValueType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableTypesFactory.hpp>
#include <QueryCompiler/GeneratableTypes/PointerDataType.hpp>
#include <QueryCompiler/GeneratableTypes/ReferenceDataType.hpp>
#include <QueryCompiler/GeneratableTypes/UserDefinedDataType.hpp>

namespace NES::QueryCompilation {
GeneratableDataTypePtr GeneratableTypesFactory::createDataType(const DataTypePtr& type) {
    auto const physicalType = DefaultPhysicalTypeFactory().getPhysicalType(type);
    if (type->isArray()) {
        auto const arrayType = DataType::as<ArrayType>(type);
        auto const componentDataType = createDataType(arrayType->component);
        auto const arrayPhysicalType = std::dynamic_pointer_cast<ArrayPhysicalType>(physicalType);
        return std::make_shared<ArrayGeneratableType>(arrayPhysicalType, componentDataType);
    }
    auto const basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
    return std::make_shared<BasicGeneratableType>(basicPhysicalType);
}

GeneratableValueTypePtr GeneratableTypesFactory::createValueType(const ValueTypePtr& valueType) {
    if (valueType->dataType->isArray()) {
        return std::make_shared<GeneratableArrayValueType>(valueType, std::dynamic_pointer_cast<ArrayValue>(valueType)->values);
    }

    return std::make_shared<GeneratableBasicValueType>(std::dynamic_pointer_cast<BasicValue>(valueType));
}

GeneratableDataTypePtr GeneratableTypesFactory::createAnonymusDataType(const std::string& type) {
    return std::make_shared<AnonymousUserDefinedDataType>(type);
}

GeneratableDataTypePtr GeneratableTypesFactory::createUserDefinedType(const StructDeclaration& structDeclaration) {
    return std::make_shared<UserDefinedDataType>(structDeclaration);
}

GeneratableDataTypePtr GeneratableTypesFactory::createPointer(const GeneratableDataTypePtr& type) {
    return std::make_shared<PointerDataType>(type);
}

GeneratableDataTypePtr GeneratableTypesFactory::createReference(const GeneratableDataTypePtr& type) {
    return std::make_shared<ReferenceDataType>(type);
}
}// namespace NES::QueryCompilation
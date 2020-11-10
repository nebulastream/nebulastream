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

#pragma once

#include <memory>
#include <string>

namespace NES {

typedef std::string Code;

class Declaration;
typedef std::shared_ptr<Declaration> DeclarationPtr;

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class GeneratableDataType;
typedef std::shared_ptr<GeneratableDataType> GeneratableDataTypePtr;

class Declaration {
  public:
    virtual const GeneratableDataTypePtr getType() const = 0;
    virtual const std::string getIdentifierName() const = 0;
    virtual const Code getTypeDefinitionCode() const = 0;
    virtual const Code getCode() const = 0;
    virtual const DeclarationPtr copy() const = 0;
    virtual ~Declaration();
};

class StructDeclaration;
const DataTypePtr createUserDefinedType(const StructDeclaration& decl);

}// namespace NES

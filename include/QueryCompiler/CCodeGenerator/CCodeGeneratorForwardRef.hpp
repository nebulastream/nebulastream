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



#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CCODEGENERATORFORWARDREF_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CCODEGENERATORFORWARDREF_HPP_
#include <memory>
namespace NES{

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class GeneratableDataType;
typedef std::shared_ptr<GeneratableDataType> GeneratableDataTypePtr;

class FunctionDefinition;
typedef std::shared_ptr<FunctionDefinition> FunctionDefinitionPtr;


class ClassDefinition;
typedef std::shared_ptr<ClassDefinition> ClassDefinitionPtr;

class Declaration;
typedef std::shared_ptr<Declaration> DeclarationPtr;

class FunctionDeclaration;
typedef std::shared_ptr<FunctionDeclaration> FunctionDeclarationPtr;

class ClassDeclaration;
typedef std::shared_ptr<ClassDeclaration> ClassDeclarationPtr;
}

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CCODEGENERATORFORWARDREF_HPP_

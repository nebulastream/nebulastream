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

#ifndef NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_CODE_GENERATOR_FORWARD_REF_HPP_
#define NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_CODE_GENERATOR_FORWARD_REF_HPP_
#include <memory>
namespace NES {

class DataType;
using DataTypePtr = std::shared_ptr<DataType>;

class ValueType;
using ValueTypePtr = std::shared_ptr<ValueType>;

class BasicValue;
using BasicValuePtr = std::shared_ptr<BasicValue>;

class AttributeField;
using AttributeFieldPtr = std::shared_ptr<AttributeField>;

namespace Windowing {

class LogicalWindowDefinition;
using LogicalWindowDefinitionPtr = std::shared_ptr<LogicalWindowDefinition>;

}// namespace Windowing

namespace QueryCompilation {

using Code = std::string;

class GeneratedCode;
using GeneratedCodePtr = std::shared_ptr<GeneratedCode>;

class CodeGenerator;
using CodeGeneratorPtr = std::shared_ptr<CodeGenerator>;

class TranslateToLegacyExpression;
using TranslateToLegacyExpressionPtr = std::shared_ptr<TranslateToLegacyExpression>;

class PipelineContext;
using PipelineContextPtr = std::shared_ptr<PipelineContext>;

class Predicate;
using PredicatePtr = std::shared_ptr<Predicate>;

class GeneratableTypesFactory;
using CompilerTypesFactoryPtr = std::shared_ptr<GeneratableTypesFactory>;

class LegacyExpression;
using LegacyExpressionPtr = std::shared_ptr<LegacyExpression>;

class CodeExpression;
using CodeExpressionPtr = std::shared_ptr<CodeExpression>;

class GeneratableDataType;
using GeneratableDataTypePtr = std::shared_ptr<GeneratableDataType>;

class GeneratableValueType;
using GeneratableValueTypePtr = std::shared_ptr<GeneratableValueType>;

class FunctionDefinition;
using FunctionDefinitionPtr = std::shared_ptr<FunctionDefinition>;

class ConstructorDefinition;
using ConstructorDefinitionPtr = std::shared_ptr<ConstructorDefinition>;

class ConstructorDeclaration;
using ConstructorDeclarationPtr = std::shared_ptr<ConstructorDeclaration>;

class AssignmentStatement;

class StructDeclaration;
using StructDeclarationPtr = std::shared_ptr<StructDeclaration>;

class NamespaceDefinition;
using NamespaceDefinitionPtr = std::shared_ptr<NamespaceDefinition>;

class ClassDefinition;
using ClassDefinitionPtr = std::shared_ptr<ClassDefinition>;

class Declaration;
using DeclarationPtr = std::shared_ptr<Declaration>;

class FunctionDeclaration;
using FunctionDeclarationPtr = std::shared_ptr<FunctionDeclaration>;

class ClassDeclaration;
using ClassDeclarationPtr = std::shared_ptr<ClassDeclaration>;

class NamespaceDeclaration;
using NamespaceDeclarationPtr = std::shared_ptr<NamespaceDeclaration>;

class Statement;
using StatementPtr = std::shared_ptr<Statement>;

class BinaryOperatorStatement;

class FunctionCallStatement;
using FunctionCallStatementPtr = std::shared_ptr<FunctionCallStatement>;

class CompoundStatement;
using CompoundStatementPtr = std::shared_ptr<CompoundStatement>;

class VarRefStatement;
using VarRefStatementPtr = std::shared_ptr<VarRefStatement>;

class VariableDeclaration;
using VariableDeclarationPtr = std::shared_ptr<VariableDeclaration>;

class ExpressionStatement;
using ExpressionStatementPtr = std::shared_ptr<ExpressionStatement>;

class BlockScopeStatement;
using BlockScopeStatementPtr = std::shared_ptr<BlockScopeStatement>;

class RecordHandler;
using RecordHandlerPtr = std::shared_ptr<RecordHandler>;
}// namespace QueryCompilation
}// namespace NES

#endif  // NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_CODE_GENERATOR_FORWARD_REF_HPP_

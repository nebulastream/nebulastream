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
namespace NES {

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class ValueType;
typedef std::shared_ptr<ValueType> ValueTypePtr;

class BasicValue;
typedef std::shared_ptr<BasicValue> BasicValuePtr;

class AttributeField;
typedef std::shared_ptr<AttributeField> AttributeFieldPtr;

namespace Windowing {

class LogicalWindowDefinition;
typedef std::shared_ptr<LogicalWindowDefinition> LogicalWindowDefinitionPtr;

}// namespace Windowing

namespace QueryCompilation {

typedef std::string Code;

class GeneratedCode;
typedef std::shared_ptr<GeneratedCode> GeneratedCodePtr;

class CodeGenerator;
typedef std::shared_ptr<CodeGenerator> CodeGeneratorPtr;

class TranslateToLegacyExpression;
typedef std::shared_ptr<TranslateToLegacyExpression> TranslateToLegacyExpressionPtr;

class PipelineContext;
typedef std::shared_ptr<PipelineContext> PipelineContextPtr;

class Predicate;
typedef std::shared_ptr<Predicate> PredicatePtr;

class GeneratableTypesFactory;
typedef std::shared_ptr<GeneratableTypesFactory> CompilerTypesFactoryPtr;

class LegacyExpression;
typedef std::shared_ptr<LegacyExpression> LegacyExpressionPtr;

class CodeExpression;
typedef std::shared_ptr<CodeExpression> CodeExpressionPtr;

class GeneratableDataType;
typedef std::shared_ptr<GeneratableDataType> GeneratableDataTypePtr;

class GeneratableValueType;
typedef std::shared_ptr<GeneratableValueType> GeneratableValueTypePtr;

class FunctionDefinition;
typedef std::shared_ptr<FunctionDefinition> FunctionDefinitionPtr;

class ConstructorDefinition;
typedef std::shared_ptr<ConstructorDefinition> ConstructorDefinitionPtr;

class ConstructorDeclaration;
typedef std::shared_ptr<ConstructorDeclaration> ConstructorDeclarationPtr;

class AssignmentStatement;

class StructDeclaration;
typedef std::shared_ptr<StructDeclaration> StructDeclarationPtr;

class NamespaceDefinition;
typedef std::shared_ptr<NamespaceDefinition> NamespaceDefinitionPtr;

class ClassDefinition;
typedef std::shared_ptr<ClassDefinition> ClassDefinitionPtr;

class Declaration;
typedef std::shared_ptr<Declaration> DeclarationPtr;

class FunctionDeclaration;
typedef std::shared_ptr<FunctionDeclaration> FunctionDeclarationPtr;

class ClassDeclaration;
typedef std::shared_ptr<ClassDeclaration> ClassDeclarationPtr;

class NamespaceDeclaration;
typedef std::shared_ptr<NamespaceDeclaration> NamespaceDeclarationPtr;

class Statement;
typedef std::shared_ptr<Statement> StatementPtr;

class BinaryOperatorStatement;

class FunctionCallStatement;
typedef std::shared_ptr<FunctionCallStatement> FunctionCallStatementPtr;

class CompoundStatement;
typedef std::shared_ptr<CompoundStatement> CompoundStatementPtr;

class VarRefStatement;
typedef std::shared_ptr<VarRefStatement> VarRefStatementPtr;

class VariableDeclaration;
typedef std::shared_ptr<VariableDeclaration> VariableDeclarationPtr;

class ExpressionStatment;
typedef std::shared_ptr<ExpressionStatment> ExpressionStatmentPtr;

class BlockScopeStatement;
typedef std::shared_ptr<BlockScopeStatement> BlockScopeStatementPtr;

class RecordHandler;
typedef std::shared_ptr<RecordHandler> RecordHandlerPtr;
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CCODEGENERATORFORWARDREF_HPP_

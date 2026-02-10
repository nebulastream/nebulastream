/*
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

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>
#include <AntlrSQLParser.h>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>

namespace NES
{
using Literal = std::variant<std::string, int64_t, uint64_t, double, bool>;
using ConfigMap = std::unordered_map<std::string, std::unordered_map<std::string, std::variant<Literal, Schema>>>;

std::string bindIdentifier(AntlrSQLParser::StrictIdentifierContext* strictIdentifier);
std::string bindIdentifier(AntlrSQLParser::IdentifierContext* identifier);

ConfigMap bindConfigOptions(const std::vector<AntlrSQLParser::NamedConfigExpressionContext*>& configOptions);
std::unordered_map<std::string, std::string> getParserConfig(const ConfigMap& configOptions);
std::unordered_map<std::string, std::string> getSourceConfig(const ConfigMap& configOptions);
std::unordered_map<std::string, std::string> getSinkConfig(const ConfigMap& configOptions);
std::optional<Schema> getSourceSchema(ConfigMap configOptions);
std::optional<Schema> getSinkSchema(ConfigMap configOptions);

Literal bindLiteral(AntlrSQLParser::ConstantContext* literalAST);
bool bindBooleanLiteral(AntlrSQLParser::BooleanLiteralContext* booleanLiteral);
double bindDoubleLiteral(AntlrSQLParser::FloatLiteralContext* doubleLiteral);
uint64_t bindUnsignedIntegerLiteral(AntlrSQLParser::UnsignedIntegerLiteralContext* unsignedIntegerLiteral);
int64_t bindIntegerLiteral(AntlrSQLParser::SignedIntegerLiteralContext* signedIntegerLiteral);
int64_t bindIntegerLiteral(AntlrSQLParser::IntegerLiteralContext* integerLiteral);
std::string bindStringLiteral(AntlrSQLParser::StringLiteralContext* stringLiteral);

Schema bindSchema(AntlrSQLParser::SchemaDefinitionContext* schemaDefAST);

DataType bindDataType(AntlrSQLParser::TypeDefinitionContext* typeDefAST);

std::string literalToString(const Literal& literal);

}

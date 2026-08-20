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

#include <memory>
#include <variant>
#include <vector>

#include <AntlrSQLParser.h>

#include <Model/ParsedTestFile.hpp>
#include <Rewriter/SqlParse.hpp>

namespace NES
{

struct LogicalSourceDeclaration
{
    AntlrSQLParser::CreateLogicalSourceDefinitionContext* definition;
};

struct PhysicalSourceDeclaration
{
    AntlrSQLParser::CreatePhysicalSourceDefinitionContext* definition;
    /// Null when the source produces its own rows (e.g., `GeneratorSource`) instead of reading attached data.
    /// A pointer into the test file's statement, which outlives one rewrite.
    const AttachedData* attached = nullptr;
};

struct SinkDeclaration
{
    AntlrSQLParser::CreateSinkDefinitionContext* definition;
};

struct ModelDeclaration
{
    AntlrSQLParser::CreateModelDefinitionContext* definition;
};

using CreateDeclaration = std::variant<LogicalSourceDeclaration, PhysicalSourceDeclaration, SinkDeclaration, ModelDeclaration>;

/// A pointer holds the parse, because a parse refers to its own members and cannot be moved.
/// The declaration points into that parse and the statement into the caller's test file, so both outlive one rewrite.
struct ClassifiedCreate
{
    const CreateStatement* create;
    std::unique_ptr<SqlParse> parse;
    CreateDeclaration declaration;
};

/// A CREATE is parsed and classified, because both later phases need that.
/// The other statements pass through classification as bare pointers,
/// because rewriting them needs the qualified names, which do not exist after classification.
/// The test file owning them outlives every phase of one rewrite, so this is safe.
using ClassifiedStatement = std::variant<ClassifiedCreate, const SelectStatement*, const DifferentialStatement*, const ExplainStatement*>;

struct ClassifiedTest
{
    std::vector<ClassifiedStatement> statements;
    /// A file containing an EXPLAIN submits its sink declarations (they can't be turned to anonymous sinks),
    /// because the test author needs to match the expectation on the declared names.
    bool containsExplain = false;
};

/// Parses and classifies every CREATE statement of the file, in the order the test wrote them.
[[nodiscard]] ClassifiedTest classifyStatements(const ParsedTestFile& testFile);

}

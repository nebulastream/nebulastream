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
#include <optional>
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
    /// Empty when the source produces its own rows (e.g., `GeneratorSource`) instead of reading attached data.
    std::optional<AttachedData> attached;
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

/// A pointer holds the parse, because a parse points at its own members and cannot move.
/// The declaration points into that parse, so the two share one lifetime.
struct ClassifiedCreate
{
    std::unique_ptr<SqlParse> parse;
    CreateDeclaration declaration;
};

/// A statement that becomes one test case.
using TestCaseStatement = std::variant<SelectStatement, DifferentialStatement, ExplainStatement>;

/// The statements of one test file, split into the parsed CREATEs and the statements that become test cases.
/// The test cases are not parsed yet, because rewriting them needs the names that the declaring phase registers from the CREATEs.
struct ClassifiedTestFile
{
    /// Every CREATE of the file, in file order.
    std::vector<ClassifiedCreate> setup;
    /// Every other statement of the file, in file order.
    std::vector<TestCaseStatement> testCases;
    /// A file with an EXPLAIN submits its declared sinks instead of inlining them, because the expected plan refers to them by name.
    /// An EXPLAIN starts no query, so those sinks never write.
    bool containsExplain = false;
};

/// Parses every CREATE statement of the file and moves every other statement into the test cases, in file order.
/// Takes the file by value, because the statements move into the result.
[[nodiscard]] ClassifiedTestFile classifyStatements(ParsedTestFile testFile);

}

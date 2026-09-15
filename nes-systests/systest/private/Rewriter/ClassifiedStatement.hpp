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
using CaseStatement = std::variant<SelectStatement, DifferentialStatement, ExplainStatement>;

/// The statements of one test file, split into the parsed CREATEs and the statements that become cases.
/// The cases wait here unchanged, because rewriting them needs the qualified names, which the declaring phase derives
/// from the CREATEs.
struct ClassifiedTestFile
{
    /// Every CREATE of the file, in the order the test wrote them.
    /// Declaring registers the names of all of them before any statement is rewritten,
    /// so the order of a CREATE relative to a case does not matter.
    std::vector<ClassifiedCreate> setup;
    /// Every other statement of the file, in the order the test wrote them.
    std::vector<CaseStatement> cases;
    /// A file containing an EXPLAIN submits its sink declarations (they can't be turned to anonymous sinks),
    /// because the test author needs to match the expectation on the declared names, not mangled ones.
    bool containsExplain = false;
};

/// Parses every CREATE statement of the file and moves every other statement into the cases, in the order the test wrote them.
/// Takes the file, because the statements move on into the classified file rather than being copied.
[[nodiscard]] ClassifiedTestFile classifyStatements(ParsedTestFile testFile);

}

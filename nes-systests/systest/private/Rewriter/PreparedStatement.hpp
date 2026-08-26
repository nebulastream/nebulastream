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
#include <Parser/SystestParser.hpp>
#include <Parser/TestFileParser.hpp>
#include <Rewriter/ParsedStatement.hpp>

namespace NES
{

/// The declaration structs are declared beside the other statement model types, because the statement binder
/// declares same-named types directly in the enclosing namespace and one program must not hold both definitions.
namespace Systest
{

/// What a CREATE declares, which decides what the rewriter emits in its place.
/// Only a physical source takes attached data, so only that alternative holds a field for it.
struct CreateLogicalSourceStatement
{
    AntlrSQLParser::CreateLogicalSourceDefinitionContext* definition;
};

struct CreatePhysicalSourceStatement
{
    AntlrSQLParser::CreatePhysicalSourceDefinitionContext* definition;
    /// Absent when the source produces its own rows from its own options instead of reading attached data.
    std::optional<AttachedData> attached;
};

struct CreateSinkStatement
{
    AntlrSQLParser::CreateSinkDefinitionContext* definition;
};

struct CreateModelStatement
{
    AntlrSQLParser::CreateModelDefinitionContext* definition;
};

}

/// What one CREATE statement declares, classified from its parse tree.
using ClassifiedCreate = std::variant<
    Systest::CreateLogicalSourceStatement,
    Systest::CreatePhysicalSourceStatement,
    Systest::CreateSinkStatement,
    Systest::CreateModelStatement>;

/// One CREATE statement, its parse tree, and what that tree says the statement declares.
/// A pointer holds the parse, because a parse refers to its own members and cannot be moved.
/// The declaration points into that parse and the statement into the caller's test file, so both outlive one rewrite.
struct PreparedCreate
{
    const Systest::CreateStatement* create;
    std::unique_ptr<ParsedStatement> parsed;
    ClassifiedCreate classified;
};

;

/// One statement of the test file, ready for the phases that follow, in the order the test wrote them.
/// A CREATE is parsed and classified here, because both later phases need that.
/// A query is only referenced, because rewriting it needs the sealed names, which do not exist yet.
/// The statements that pass through preparation unchanged stay owned by the test file.
/// The test file outlives every phase of one rewrite, so these references stay valid without ownership here.
using PreparedQuery = const Systest::QueryStatement*;
using PreparedDifferential = const Systest::DifferentialStatement*;
using PreparedExplain = const Systest::ExplainStatement*;

using PreparedStatement = std::variant<PreparedCreate, PreparedQuery, PreparedDifferential, PreparedExplain>;

/// Parses and classifies every CREATE statement of the file, in the order the test wrote them.
std::vector<PreparedStatement> prepare(const TestFile& testFile);

}

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

#include <filesystem>
#include <string>
#include <unordered_map>
#include <vector>

#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Model/RewrittenTest.hpp>
#include <Parser/TestFileParser.hpp>
#include <Rewriter/NameQualifier.hpp>

namespace NES
{

/// Context of one test file's rewrite.
/// The key qualifies the catalog-visible names, the working directory takes the generated data and result files, and the
/// test data directory holds the files that the test refers to.
/// Both directories are absolute paths on the host running this process, and a worker resolves them on its own, so a run
/// reaching workers in other processes needs those paths to reach the same files there.
/// The source host and the sink host are separate fields, because a run on a cluster may place sources and sinks
/// on different workers, respectively.
struct RewriteTarget
{
    std::string testFileKey;
    /// This file's display name in failures and progress lines, which discovery chose so that two files that
    /// share a stem in different folders do not report the same.
    std::string displayName;
    std::filesystem::path workingDir;
    std::filesystem::path testDataDir;
    Host sourceHost;
    Host sinkHost;
};

/// A sink declaration that is stored for inlining.
struct SinkDefinition
{
    std::string type;
    std::string schema;
};

using SinkByName = std::unordered_map<Identifier, SinkDefinition>;

/// Adds config options to a physical source statement the rewriter already emitted.
/// The new options merge into the statement's single `SET` clause, keeping the options already there.
/// The runner needs this for a value that is known only once the run started, such as the port that a data server bound.
std::string addSourceOptions(const std::string& sql, const std::vector<std::string>& options);

/// Rewrites the statements of one test file.
/// Every test file of one invocation shares the catalog, so the rewriter keeps their names apart.
/// - It prefixes every source name with the test file key.
/// - It inlines each declared sink into its query with a dedicated result.
/// - It derives each query's name from its location.
class SqlRewriter
{
public:
    explicit SqlRewriter(RewriteTarget target);

    /// Rewrites a parsed test file into the setup DDL and the queries to run and check.
    /// The first phase parses and classifies every CREATE statement.
    /// The second declares every name that becomes catalog-visible.
    /// The third emits the setup DDL and rewrites the queries, in file order.
    [[nodiscard]] RewrittenTest rewrite(const TestFile& testFile) const;

private:
    RewriteTarget target;
};

}

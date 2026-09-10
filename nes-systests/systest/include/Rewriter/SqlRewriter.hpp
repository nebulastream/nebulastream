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

#include <cstddef>
#include <filesystem>
#include <string>
#include <unordered_map>
#include <vector>

#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Model/RunnableTest.hpp>
#include <Parser/TestFileParser.hpp>
#include <Rewriter/NameQualifier.hpp>

namespace NES
{

/// What the rewriter builds the SQL of one test file against.
/// The key qualifies the catalog-visible names, the working directory takes the generated data and result files, and the
/// test data directory holds the files the test refers to.
/// Both directories are absolute paths on the host running this process, and a worker resolves them on its own, so a run
/// reaching workers in other processes needs those paths to reach the same files there.
/// Sources and sinks take their worker separately, because a topology may allow them on different ones.
struct RewriteTarget
{
    std::string testFileKey;
    /// The name a failure and a progress line report this file under, which discovery settled so that two files
    /// sharing a stem in different folders do not report the same.
    std::string displayName;
    std::filesystem::path workingDir;
    std::filesystem::path testDataDir;
    Host sourceHost;
    Host sinkHost;
};

/// A sink declaration held for inlining: the type it declares, such as File or Checksum, and its schema definition text.
struct SinkDefinition
{
    std::string type;
    std::string schema;
};

using SinkByName = std::unordered_map<Identifier, SinkDefinition>;

/// Adds config options to a physical source statement the rewriter already emitted.
/// The new options merge into the statement's single `SET` clause, keeping the options already there.
/// The runner needs this for a value known only once the run started, such as the port a data server bound.
std::string addSourceOptions(const std::string& sql, const std::vector<std::string>& options);

/// Rewrites the statements of one test file into SQL the coordinator accepts.
/// Every test file of one invocation shares the coordinator's catalog, so the rewriter keeps their names apart.
/// It prefixes every source name with the test file key.
/// It inlines each declared sink into its query with a result file of its own.
/// It derives each query's name from its location.
class SqlRewriter
{
public:
    explicit SqlRewriter(RewriteTarget target);

    /// Rewrites a parsed test file into the setup DDL and the queries to run and check.
    /// The first phase parses and classifies every CREATE statement.
    /// The second declares every name that becomes catalog-visible.
    /// The third emits the setup DDL and rewrites the queries, in file order.
    [[nodiscard]] RunnableTest rewrite(const TestFile& testFile);

private:
    /// One CREATE statement, parsed and classified once, so the two later phases share that work.
    /// Defined in the implementation, because classifying reads the parse tree.
    struct PreparedCreate;

    /// Registers the catalog-visible names one CREATE statement declares.
    /// Runs for every CREATE before any rewriting, so the rewriter qualifies a reference wherever it appears in the file.
    void declareNames(PreparedCreate& prepared, NameRegistry& registry);
    /// Rewrites one CREATE statement and adds it to the setup statements.
    /// A sink adds nothing, because the declaring phase held it for the query that writes into it.
    void emitCreate(PreparedCreate& prepared, const QualifiedNames& names);

    /// Rewrites one query and returns it rather than adding it, so a differential block can pair both halves before adding either.
    /// The discriminator separates the result files of two queries that share a number, as the halves of a block do.
    /// A query that owns its number passes an empty discriminator.
    [[nodiscard]] RunnableQuery rewriteQuery(
        const std::string& sql, SystestQueryId id, Expectation expected, const QualifiedNames& names, std::string_view resultDiscriminator);

    /// Adds an EXPLAIN, which answers with a plan rather than with rows.
    void emitExplain(const Systest::ExplainStatement& explain, const QualifiedNames& names);

    /// Adds both halves of a differential block, in file order, each with a result file of its own.
    /// Sharing one file would compare a result against itself.
    void emitDifferential(const Systest::DifferentialStatement& block, const QualifiedNames& names);

    RewriteTarget target;

    /// Records that the sources of this logical source read this file, so a query reading it can be told which files that is.
    /// A source that generates its own rows records nothing, and a measurement then sees no input for it.
    void recordInputFile(const Identifier& logicalSource, const std::filesystem::path& dataFile);

    /// Fills in the data files every query reads, once the CREATE statements have said which file belongs to which source.
    /// A separate pass, because a query may read a source the test file declares below it.
    void resolveInputFiles();

    /// State the rewriter builds up while rewriting a single test file.
    /// The names are not here, because the declaring phase takes the registry and the emitting phase takes the sealed names.
    SinkByName sinkByName;

    /// Whether this test file declares its sinks instead of inlining them, which a file holding an EXPLAIN does.
    bool declaresSinks = false;
    size_t sourceOrdinal = 0;

    /// The data files each logical source reads, and the sources each query read, kept apart until both are known.
    std::unordered_map<Identifier, std::vector<std::filesystem::path>> inputFilesBySource;
    std::vector<std::vector<Identifier>> sourcesPerQuery;
    RunnableTest runnable;
};

}

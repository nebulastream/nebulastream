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

#include <Model/ParsedTestFile.hpp>
#include <Model/RewrittenTest.hpp>
#include <Rewriter/RewriteTarget.hpp>

namespace NES
{

/// Rewrites the statements of one test file.
/// Creates a `RewrittenTest`, consisting of rewritten setup statements and rewritten cases (i.e., queries)
/// Because every test of one invocation shares the catalog, we need to disambiguate the names defined in the tests.
/// This disambiguation and the insertion of the correct file path for the result file is the goal of the rewriter.
/// To do this, it applies the following steps:
/// - Prefixing every logical source name with the test file key (to avoid collisions among files).
/// - Inlining each declared sink into its query with a dedicated result file path.
/// - Deriving each query's name from its location.
///
/// The first phase parses and classifies every CREATE statement.
/// The second declares every name (by prefixing it with the test file key) that becomes catalog-visible.
/// The third emits the setup DDL and rewrites the queries, in file order.
[[nodiscard]] RewrittenTest rewriteTestFile(const ParsedTestFile& testFile, const RewriteTarget& target);

}

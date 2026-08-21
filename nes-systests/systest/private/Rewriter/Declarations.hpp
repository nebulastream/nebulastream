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

#include <string>
#include <vector>
#include <Rewriter/NameQualifier.hpp>
#include <Rewriter/PreparedStatement.hpp>
#include <Rewriter/SqlRewriter.hpp>

namespace NES
{

/// Everything the declaring phase learned about one test file, consumed by the emitting phase.
struct Declarations
{
    QualifiedNames names;
    SinkByName sinkByName;
    /// Whether sink names were registered and their declarations are to be submitted, which a file that contains
    /// an EXPLAIN needs so a plan can print a declared sink's name.
    bool declaresSinks = false;
};

/// Registers every catalog-visible name of the file before any statement is rewritten against it.
/// Rewriting substitutes only registered names, so this order makes the result independent of where a name is declared.
Declarations declareAll(std::vector<PreparedStatement>& prepared, const std::string& testFileKey);

}

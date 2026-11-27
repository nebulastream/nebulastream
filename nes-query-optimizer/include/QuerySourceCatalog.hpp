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

#include <expected>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// A query source is a member of a logical source that is defined by a query fragment instead of a physical source.
/// Example: `SELECT *, 'host-x' AS host FROM Generator(...) INTO HEART_BEAT` makes the fragment
/// `SELECT *, 'host-x' AS host FROM Generator(...)` a member of the logical source HEART_BEAT.
///
/// The LogicalSourceExpansionRule expands a logical source to the union of its physical sources and its query sources.
/// A fragment can read other logical sources; the expansion then continues recursively.
/// The fragment is stored unbound (as parsed); the optimizer binds and type-checks it together with the query that reads it.
class QuerySourceCatalog
{
public:
    /// Adds the fragment as a member of the logical source.
    /// Fails if the fragment reads the logical source, directly or through other query sources (a cycle).
    [[nodiscard]] std::expected<void, Exception> addQuerySource(const Identifier& logicalSourceName, LogicalOperator fragment);

    /// @return the fragments of all query sources of the logical source, empty if it has none
    [[nodiscard]] std::vector<LogicalOperator> getQuerySources(const Identifier& logicalSourceName) const;

private:
    /// Must be called while holding the mutex.
    [[nodiscard]] bool reaches(const Identifier& from, const Identifier& target) const;

    mutable std::mutex mutex;
    std::unordered_map<Identifier, std::vector<LogicalOperator>> querySources;
};

}

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
#include "LogicalOperator.hpp"

namespace NES
{

///@brief Indicates that an operator writes a different value into fields with the same name as fields that it reads from
///
/// For example, a projection:
/// @code{sql}
/// SELECT attr1 + 1 as attr1, attr1 + attr2 as attr2 FROM r
/// @endcode
///
/// Used by @link DecideFieldMappings @endlink to identify operators that might internally have a read-write conflict between fields.
class Reprojecter : public Castable<Reprojecter>
{
public:
    ///@return A mapping of all output fields to fields from the child operators that are accessed while calculating the output fields.
    ///For example, for a projection
    /// @code{sql}
    /// SELECT attr1 + 1 as attr1, attr1 + attr2 as attr2 FROM r
    /// @endcode
    /// This should yield {(attr1, {attr1}), (attr2, {attr1, attr2})}
    ///
    /// An implementing operator can leave out trivial projections to avoid unnecessary renames/copies of columns.
    /// Trivial projections are projections that just forward a field without any modification.
    [[nodiscard]] virtual std::unordered_map<Field, std::unordered_set<Field>> getAccessedFieldsForOutput() const = 0;
};
}
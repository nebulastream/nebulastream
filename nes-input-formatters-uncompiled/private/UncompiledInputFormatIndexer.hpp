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

#include <Concepts.hpp>

namespace NES
{

/// Implements format-specific (CSV, JSON, Protobuf, etc.) indexing of raw buffers.
/// The InputFormatIndexerTask uses the UncompiledInputFormatIndexer to determine byte offsets of all fields of a given tuple and all tuples of a given buffer.
/// The offsets allow the InputFormatIndexerTask to parse only the fields that it needs to for the particular query.
/// @Note All UncompiledInputFormatIndexer implementations must be thread-safe. NebulaStream's query engine concurrently executes InputFormatIndexerTasks.
///       Thus, the InputFormatIndexerTask calls the interface functions of the UncompiledInputFormatIndexer concurrently.
template <typename T>
class UncompiledInputFormatIndexer
{
    friend T;

private:
    /// We can't constrain 'T' using the UncompiledInputFormatIndexerType concept, since it is not satisfied when the 'UncompiledInputFormatIndexer' is initiated.
    /// Thus, we delay asserting the constraints of the concept by applying it in the constructor.
    UncompiledInputFormatIndexer()
    {
        /// Assert that the UncompiledInputFormatIndexer implementation adheres to the UncompiledInputFormatIndexerType concept, which imposes an interface on
        /// all InputFormatIndexers.
        static_assert(UncompiledInputFormatIndexerType<T>);
    }
};
}

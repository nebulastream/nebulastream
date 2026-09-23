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

#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <Operators/Statistic/StatisticBlobType.hpp>
#include <Statistics/StatisticIterator.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

using StatisticIteratorRegistryReturnType = std::shared_ptr<StatisticIterator>;

struct StatisticIteratorRegistryArguments
{
    /// The blob type the probe declared, i.e. the name of the aggregation that wrote the statistic. It is also the
    /// registry key, so an entry could hard-code it -- but a decoder shared by several names needs to know which one
    /// it was asked for, and StatisticIterator carries it anyway.
    StatisticBlobType typeName;
    /// The columns the probe declared, in the order it declared them, resolved to their physical field names.
    /// Position is what gives them their meaning; the user chooses the names.
    std::vector<StatisticPayloadField> payloadFields;
};

/// A factory validates the declared payload fields against what its blob can produce and throws InvalidQuerySyntax
/// naming the expected call when they do not fit. The columns come from the query, so that is a user error and must
/// not be an INVARIANT.
using StatisticIteratorFn = std::function<StatisticIteratorRegistryReturnType(StatisticIteratorRegistryArguments)>;

/// The decoders for stored statistic payloads, keyed by blob type. A synopsis registers the decoder that reads the
/// blob its aggregation writes; every name without an entry is a scalar and falls back to ScalarStatisticIterator
/// (see StatisticIteratorProvider).
class StatisticIteratorRegistry
    : public RuntimeRegistry<StatisticIteratorRegistry, std::string, StatisticIteratorFn, /*CaseSensitive*/ false>
{
public:
    static StatisticIteratorRegistry& instance();
};

}

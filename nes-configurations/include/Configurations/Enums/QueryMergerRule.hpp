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

#ifndef NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_ENUMS_QUERYMERGERRULE_HPP_
#define NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_ENUMS_QUERYMERGERRULE_HPP_
#include <cstdint>

namespace NES::Optimizer {
enum class QueryMergerRule : uint8_t {
    SyntaxBasedCompleteQueryMergerRule,
    SyntaxBasedPartialQueryMergerRule,
    Z3SignatureBasedCompleteQueryMergerRule,
    Z3SignatureBasedPartialQueryMergerRule,
    Z3SignatureBasedPartialQueryMergerBottomUpRule,
    HashSignatureBasedCompleteQueryMergerRule,
    ImprovedHashSignatureBasedCompleteQueryMergerRule,
    ImprovedHashSignatureBasedPartialQueryMergerRule,
    HashSignatureBasedPartialQueryMergerRule,
    DefaultQueryMergerRule,
    HybridCompleteQueryMergerRule,
    Z3SignatureBasedBottomUpQueryContainmentRule,
    Z3SignatureBasedTopDownQueryContainmentMergerRule
};
}
#endif // NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_ENUMS_QUERYMERGERRULE_HPP_

/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_QUERYMERGERPHASE_HPP
#define NES_QUERYMERGERPHASE_HPP

#include <Optimizer/QueryMerger/BaseQueryMergerRule.hpp>
#include <iostream>

namespace NES::Optimizer {

enum class QueryMergerRule {
    SyntaxBasedCompleteQueryMergerRule,
    SyntaxBasedPartialQueryMergerRule,
    Z3SignatureBasedCompleteQueryMergerRule,
    Z3SignatureBasedPartialQueryMergerRule,
    StringSignatureBasedCompleteQueryMergerRule,
    ImprovedStringSignatureBasedCompleteQueryMergerRule,
    StringSignatureBasedPartialQueryMergerRule
};

static const std::map<std::string, QueryMergerRule> stringToMergerRuleEnum{
    {"SyntaxBasedCompleteQueryMergerRule", QueryMergerRule::SyntaxBasedCompleteQueryMergerRule},
    {"SyntaxBasedPartialQueryMergerRule", QueryMergerRule::SyntaxBasedPartialQueryMergerRule},
    {"Z3SignatureBasedCompleteQueryMergerRule", QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule},
    {"Z3SignatureBasedPartialQueryMergerRule", QueryMergerRule::Z3SignatureBasedPartialQueryMergerRule},
    {"StringSignatureBasedCompleteQueryMergerRule", QueryMergerRule::StringSignatureBasedCompleteQueryMergerRule},
    {"ImprovedStringSignatureBasedCompleteQueryMergerRule", QueryMergerRule::ImprovedStringSignatureBasedCompleteQueryMergerRule},
    {"StringSignatureBasedPartialQueryMergerRule", QueryMergerRule::StringSignatureBasedPartialQueryMergerRule},
};

class QueryMergerPhase;
typedef std::shared_ptr<QueryMergerPhase> QueryMergerPhasePtr;

class SyntaxBasedCompleteQueryMergerRule;
typedef std::shared_ptr<SyntaxBasedCompleteQueryMergerRule> SyntaxBasedCompleteQueryMergerRulePtr;

class Z3SignatureBasedCompleteQueryMergerRule;
typedef std::shared_ptr<Z3SignatureBasedCompleteQueryMergerRule> Z3SignatureBasedCompleteQueryMergerRulePtr;

class QueryMergerPhase {

  public:
    static QueryMergerPhasePtr create(z3::ContextPtr context, std::string queryMergerRuleName);

    /**
     * @brief execute method to apply different query merger rules on the global query plan.
     * @param globalQueryPlan: the global query plan
     * @return true if successful
     */
    bool execute(GlobalQueryPlanPtr globalQueryPlan);

  private:
    explicit QueryMergerPhase(z3::ContextPtr context, std::string queryMergerRuleName);
    BaseQueryMergerRulePtr queryMergerRule;
};
}// namespace NES::Optimizer
#endif//NES_QUERYMERGERPHASE_HPP

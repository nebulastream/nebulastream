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

class QueryMergerPhase;
typedef std::shared_ptr<QueryMergerPhase> QueryMergerPhasePtr;

class SyntaxBasedCompleteQueryMergerRule;
typedef std::shared_ptr<SyntaxBasedCompleteQueryMergerRule> SyntaxBasedCompleteQueryMergerRulePtr;

class SignatureBasedCompleteQueryMergerRule;
typedef std::shared_ptr<SignatureBasedCompleteQueryMergerRule> SignatureBasedCompleteQueryMergerRulePtr;

class QueryMergerPhase {

  public:
    enum class MergerRule {
        SyntaxBasedCompleteQueryMergerRule,
        SyntaxBasedPartialQueryMergerRule,
        Z3SignatureBasedCompleteQueryMergerRule,
        Z3SignatureBasedPartialQueryMergerRule,
        StringSignatureBasedCompleteQueryMergerRule,
        StringSignatureBasedPartialQueryMergerRule
    };

    std::map<std::string, MergerRule> stringToMergerRuleEnum{
        {"SyntaxBasedCompleteQueryMergerRule", MergerRule::SyntaxBasedCompleteQueryMergerRule},
        {"SyntaxBasedPartialQueryMergerRule", MergerRule::SyntaxBasedPartialQueryMergerRule},
        {"Z3SignatureBasedCompleteQueryMergerRule", MergerRule::Z3SignatureBasedCompleteQueryMergerRule},
        {"Z3SignatureBasedPartialQueryMergerRule", MergerRule::Z3SignatureBasedPartialQueryMergerRule},
        {"StringSignatureBasedCompleteQueryMergerRule", MergerRule::StringSignatureBasedCompleteQueryMergerRule},
        {"StringSignatureBasedPartialQueryMergerRule", MergerRule::StringSignatureBasedPartialQueryMergerRule},
    };

    static QueryMergerPhasePtr create(std::string queryMergerRuleName);

    /**
     * @brief execute method to apply different query merger rules on the global query plan.
     * @param globalQueryPlan: the global query plan
     * @return true if successful
     */
    bool execute(GlobalQueryPlanPtr globalQueryPlan);

  private:
    explicit QueryMergerPhase(std::string queryMergerRuleName);
    BaseQueryMergerRulePtr queryMergerRule;
};
}// namespace NES::Optimizer
#endif//NES_QUERYMERGERPHASE_HPP

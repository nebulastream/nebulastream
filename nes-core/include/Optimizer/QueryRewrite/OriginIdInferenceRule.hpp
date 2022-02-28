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

#ifndef NES_INCLUDE_OPTIMIZER_QUERYREWRITE_ORIGINIDINFERENCERULE_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERYREWRITE_ORIGINIDINFERENCERULE_HPP_

#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>
#include <Configurations/Coordinator/OptimizerConfiguration.hpp>

namespace NES::Optimizer {

class OriginIdInferenceRule;
using OriginIdInferenceRulePtr = std::shared_ptr<OriginIdInferenceRule>;

/**
 * @brief This rule will replace the logical window operator with either a centralized or distributed implementation.
 * The following rule applies:
 *      - if the logical window operators has more than one child, we will replace it with a distributed implementation, otherwise with a centralized
 *
 * Example: a query for centralized window:
 *                                          Sink
 *                                           |
 *                                           LogicalWindow
 *                                           |
 *                                        Source(Car)
     will be changed to:
 *
 *                                                  Sink
 *                                                  |
 *                                              LogicalWindow
 *                                                   |
 *                                        Source(Car)    Source(Car)
 *
 * Example: a query for distributed window:
 *
 *
 *
 *                                                 Sink
 *                                                     |
 *                                              WindowCombiner
 *                                                /     \
 *                                              /        \
 *                                    WindowSlicer        WindowSlicer
 *                                           |             |
 *                                      Source(Car1)    Source(Car2)
 * ---------------------------------------------
 * Example: a query :                       Sink
 *                                           |
 *                                           Window
 *                                           |
 *                                        Source(Car)
 *
 * will be expanded to:                        Sink
 *                                               |
 *                                          Window-Combiner
 *                                                |
 *                                          Watermark-Assigner
 *                                             /    \
 *                                           /       \
 *                            Window-SliceCreator Window-SliceCreator
 *                                      |               |
 *                             Watermark-Assigner   Watermark-Assigner
 *                                      |               |
 *                                   Source(Car1)    Source(Car2)
*/
class OriginIdInferenceRule : public BaseRewriteRule {
  public:
    static OriginIdInferenceRulePtr create();
    virtual ~OriginIdInferenceRule() = default;
    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;

  private:
    explicit OriginIdInferenceRule();
};
}// namespace NES::Optimizer
#endif  // NES_INCLUDE_OPTIMIZER_QUERYREWRITE_ORIGINIDINFERENCERULE_HPP_

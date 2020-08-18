#ifndef NES_IMPL_OPTIMIZER_QUERYREWRITE_BASERULE_HPP_
#define NES_IMPL_OPTIMIZER_QUERYREWRITE_BASERULE_HPP_

#include <Util/Logger.hpp>
#include <memory>

namespace NES {

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class BaseRewriteRule : public std::enable_shared_from_this<BaseRewriteRule> {

  public:
    /**
     * @brief Apply the rule to the Query plan
     * @param queryPlanPtr : The original query plan
     * @return The updated query plan
     */
    virtual QueryPlanPtr apply(QueryPlanPtr queryPlanPtr) = 0;

    /**
     * @brief Checks if the current node is of type RuleType
     * @tparam RefinementType
     * @return bool true if node is of RuleType
     */
    template<class RefinementType>
    const bool instanceOf() {
        if (dynamic_cast<RefinementType*>(this)) {
            return true;
        };
        return false;
    };

    /**
    * @brief Dynamically casts the node to a RuleType
    * @tparam RefinementType
    * @return returns a shared pointer of the RuleType
    */
    template<class RefinementType>
    std::shared_ptr<RefinementType> as() {
        if (instanceOf<RefinementType>()) {
            return std::dynamic_pointer_cast<RefinementType>(this->shared_from_this());
        } else {
            NES_FATAL_ERROR("We performed an invalid cast");
            throw std::bad_cast();
        }
    }
};
}// namespace NES

#endif//NES_IMPL_OPTIMIZER_QUERYREWRITE_BASERULE_HPP_

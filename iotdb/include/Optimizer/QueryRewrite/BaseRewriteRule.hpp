#ifndef NES_IMPL_OPTIMIZER_QUERYREWRITE_BASERULE_HPP_
#define NES_IMPL_OPTIMIZER_QUERYREWRITE_BASERULE_HPP_

#include <memory>
#include <Util/Logger.hpp>

namespace NES {

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class BaseRewriteRule : public std::enable_shared_from_this<BaseRewriteRule>  {

  public:

    /**
     * @brief Apply the rule to the Query plan
     * @param queryPlanPtr : The original query plan
     * @return The updated query plan
     */
    virtual QueryPlanPtr apply(QueryPlanPtr queryPlanPtr) = 0;

    /**
     * @brief Checks if the current node is of type RuleType
     * @tparam RuleType
     * @return bool true if node is of RuleType
     */
    template<class RuleType>
    const bool instanceOf() {
        if (dynamic_cast<RuleType*>(this)) {
            return true;
        };
        return false;
    };

    /**
    * @brief Dynamically casts the node to a RuleType
    * @tparam RuleType
    * @return returns a shared pointer of the RuleType
    */
    template<class RuleType>
    std::shared_ptr<RuleType> as() {
        if (instanceOf<RuleType>()) {
            return std::dynamic_pointer_cast<RuleType>(this->shared_from_this());
        } else {
            NES_FATAL_ERROR("We performed an invalid cast");
            throw std::bad_cast();
        }
    }
};
}

#endif //NES_IMPL_OPTIMIZER_QUERYREWRITE_BASERULE_HPP_

#ifndef NES_PLANIDGENERATOR_HPP
#define NES_PLANIDGENERATOR_HPP

#include <Plans/Global/Query/GlobalQueryId.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>

namespace NES {

/**
 * @brief This class is responsible for generating identifiers for query plans
 */
class PlanIdGenerator {

  public:
    /**
     * @brief Returns the next free global query Id
     * @return global query id
     */
    static GlobalQueryId getNextGlobalQueryId();

    /**
     * @brief Returns the next free query Sub Plan Id
     * @return query sub plan id
     */
    static QuerySubPlanId getNextQuerySubPlanId();

    /**
     * @brief Returns the next free Query id
     * @return query id
     */
    static QueryId getNextQueryId();
};
}// namespace NES
#endif//NES_PLANIDGENERATOR_HPP

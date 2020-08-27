#ifndef NES_INCLUDE_QUERYCOMPILER_QUERYEXECUTIONPLANID_HPP_
#define NES_INCLUDE_QUERYCOMPILER_QUERYEXECUTIONPLANID_HPP_

#include <stdint.h>

/**
 * @brief this alias represent a locally running query execution plan
 */
using QueryExecutionPlanId = uint64_t;
static constexpr uint64_t INVALID_QUERY_EXECUTION_PLAN_ID = 0;

#endif//NES_INCLUDE_QUERYCOMPILER_QUERYEXECUTIONPLANID_HPP_

#ifndef NES_INCLUDE_QUERYCOMPILER_QUERYEXECUTIONPLANID_HPP_
#define NES_INCLUDE_QUERYCOMPILER_QUERYEXECUTIONPLANID_HPP_

#include <stdint.h>

// TODO use a proper class instead of an alias to std::string
// TODO proper = do not use anything larger than 8/16 bytes (i.e., do not wrap std::string)

/**
 * @brief this alias represent a locally running query execution plan
 */
using QueryExecutionPlanId = uint64_t;

#endif//NES_INCLUDE_QUERYCOMPILER_QUERYEXECUTIONPLANID_HPP_

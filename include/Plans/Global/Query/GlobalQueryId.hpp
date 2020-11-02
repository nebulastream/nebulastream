#ifndef NES_INCLUDE_API_GLOBALQUERYID_HPP_
#define NES_INCLUDE_API_GLOBALQUERYID_HPP_

#include <stdint.h>

/**
 * @brief this alias represent a running query plan that composed of 1 or more original query plans
 */
using GlobalQueryId = uint64_t;
static constexpr uint64_t INVALID_GLOBAL_QUERY_ID = 0;

#endif//NES_INCLUDE_API_GLOBALQUERYID_HPP_

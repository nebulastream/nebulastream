#ifndef NES_INCLUDE_API_QUERYID_HPP_
#define NES_INCLUDE_API_QUERYID_HPP_

#include <stdint.h>

/**
 * @brief this alias represent a locally running query execution plan
 */
using QueryId = uint64_t;
static constexpr uint64_t INVALID_QUERY_ID = 0;

#endif//NES_INCLUDE_API_QUERYID_HPP_

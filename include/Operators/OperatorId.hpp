#ifndef NES_OPERATORID_HPP
#define NES_OPERATORID_HPP

#include <stdint.h>

/**
 * @brief this alias represent a locally running query execution plan
 */
using OperatorId = uint64_t;
static constexpr uint64_t INVALID_OPERATOR_ID = 0;

#endif//NES_OPERATORID_HPP

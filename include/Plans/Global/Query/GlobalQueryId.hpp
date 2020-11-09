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

#ifndef NES_INCLUDE_API_GLOBALQUERYID_HPP_
#define NES_INCLUDE_API_GLOBALQUERYID_HPP_

#include <stdint.h>

/**
 * @brief this alias represent a running query plan that composed of 1 or more original query plans
 */
using GlobalQueryId = uint64_t;
static constexpr uint64_t INVALID_GLOBAL_QUERY_ID = 0;

#endif//NES_INCLUDE_API_GLOBALQUERYID_HPP_

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
#ifndef NES_SOURCEID_HPP
#define NES_SOURCEID_HPP

#include <stdint.h>

/**
 * @brief this alias represent a globally unique source id
 */
using SourceId = uint64_t;
static constexpr uint64_t INVALID_SOURCE_ID = 0;

#endif//NES_SOURCEID_HPP

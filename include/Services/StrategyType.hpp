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
//
// Created by balint on 25.12.21.
//

#ifndef NES_STRATEGYTYPE_HPP
#define NES_STRATEGYTYPE_HPP

#include <stdint.h>

/**
 * @brief this alias represent a strategy type for migration
 */
using StrategyType = uint8_t;
static constexpr uint8_t INVALID_STRATEGY_TYPE = 0;

#endif//NES_STRATEGYTYPE_HPP

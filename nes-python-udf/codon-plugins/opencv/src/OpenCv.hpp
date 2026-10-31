/*
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

#pragma once

#include <cstdint>

extern "C" uint8_t nes_cv_imencode(
    const int8_t*,
    uint64_t,
    const uint8_t*,
    uint64_t,
    uint64_t,
    uint64_t,
    uint64_t,
    uint64_t,
    const int64_t*,
    uint64_t,
    int8_t**,
    uint64_t*,
    int8_t**,
    uint64_t*);
extern "C" uint8_t nes_cv_cvt_color(
    const uint8_t*,
    uint64_t,
    uint64_t,
    uint64_t,
    uint64_t,
    uint64_t,
    uint64_t,
    uint64_t,
    int8_t**,
    uint64_t*,
    uint64_t*,
    uint64_t*,
    uint64_t*,
    uint64_t*,
    int8_t**,
    uint64_t*);
extern "C" uint8_t nes_cv_apply_color_map(
    const uint8_t*,
    uint64_t,
    uint64_t,
    uint64_t,
    uint64_t,
    uint64_t,
    int8_t**,
    uint64_t*,
    uint64_t*,
    uint64_t*,
    uint64_t*,
    uint64_t*,
    int8_t**,
    uint64_t*);
extern "C" uint8_t nes_cv_resize(
    const uint8_t*,
    uint64_t,
    uint64_t,
    uint64_t,
    uint64_t,
    uint64_t,
    uint64_t,
    uint64_t,
    double,
    double,
    uint64_t,
    int8_t**,
    uint64_t*,
    uint64_t*,
    uint64_t*,
    uint64_t*,
    uint64_t*,
    int8_t**,
    uint64_t*);
extern "C" uint8_t
nes_cv_imdecode(const uint8_t*, uint64_t, int64_t, int8_t**, uint64_t*, uint64_t*, uint64_t*, uint64_t*, uint64_t*, int8_t**, uint64_t*);
extern "C" uint8_t nes_cv_warp_affine(
    const uint8_t*,
    uint64_t,
    uint64_t,
    uint64_t,
    uint64_t,
    uint64_t,
    double,
    double,
    double,
    double,
    double,
    double,
    uint64_t,
    uint64_t,
    uint64_t,
    uint64_t,
    double,
    double,
    double,
    double,
    int8_t**,
    uint64_t*,
    uint64_t*,
    uint64_t*,
    uint64_t*,
    uint64_t*,
    int8_t**,
    uint64_t*);
extern "C" uint8_t nes_cv_normalize(
    const uint8_t*,
    uint64_t,
    uint64_t,
    uint64_t,
    uint64_t,
    uint64_t,
    double,
    double,
    uint64_t,
    int64_t,
    int8_t**,
    uint64_t*,
    uint64_t*,
    uint64_t*,
    uint64_t*,
    uint64_t*,
    int8_t**,
    uint64_t*);
extern "C" uint8_t nes_cv_rectangle(
    uint8_t*,
    uint64_t,
    uint64_t,
    uint64_t,
    uint64_t,
    uint64_t,
    int64_t,
    int64_t,
    int64_t,
    int64_t,
    double,
    double,
    double,
    int64_t,
    int64_t,
    int64_t,
    int8_t**,
    uint64_t*);

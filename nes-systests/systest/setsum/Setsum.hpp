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
#include <array>
#include <atomic>
#include <cstdint>
#include <ostream>
#include <string_view>

/// Setsum - order agnostic, additive, subtractive checksum
/// Based on the algorithm from https://avi.im/blag/2025/setsum/
/// Uses 8 columns with large primes near 2^32 and MurmurHash3
struct Setsum
{
    /// 8 large prime numbers close to 2^32 (UINT32_MAX = 4294967295)
    static constexpr std::array<uint32_t, 8> PRIMES = {
        4294967291U, // 2^32 - 5
        4294967279U, // 2^32 - 17
        4294967231U, // 2^32 - 65
        4294967197U, // 2^32 - 99
        4294967189U, // 2^32 - 107
        4294967161U, // 2^32 - 135
        4294967143U, // 2^32 - 153
        4294967111U  // 2^32 - 185
    };

    /// Add data to the setsum (order-agnostic)
    void add(std::string_view data);

    /// Remove data from the setsum by adding the modular inverse
    void remove(std::string_view data);

    /// Equality comparison
    bool operator==(const Setsum&) const = default;

    /// Output stream operator for debugging
    friend std::ostream& operator<<(std::ostream& os, const Setsum& obj);

    /// 8 columns storing the setsum state (256 bits total)
    std::array<std::atomic<uint32_t>, 8> columns = {0, 0, 0, 0, 0, 0, 0, 0};
};

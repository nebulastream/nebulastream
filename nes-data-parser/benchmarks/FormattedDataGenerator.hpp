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

#ifndef FORMATTEDDATAGENERATOR_HPP
#define FORMATTEDDATAGENERATOR_HPP

#include <concepts>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>

template<typename T>
T generateValue();

template<std::floating_point T>
T generateValue() {
    static std::random_device dev;
    static std::mt19937 rng(dev());
    static std::uniform_int_distribution<std::mt19937::result_type> dist(1, 6);

    return static_cast<T>(dist(rng));
}

template<std::integral T>
size_t generateValue() {
    static_assert(!std::same_as<T, float>);
    static std::random_device dev;
    static std::mt19937 rng(dev());
    static std::uniform_int_distribution<std::mt19937::result_type> dist(1, 6);

    return dist(rng);
}

template<>
std::string generateValue() {
    return "\"aaaaaaaaaaaa\"";
}

template<typename... Ts>
std::string generateCSVData(size_t numberOfTuples) {
    std::stringstream ss;
    for (size_t i = 0; i < numberOfTuples; i++) {
        std::size_t n{0};
        ((ss << generateValue<Ts>() << (++n != sizeof...(Ts) ? "," : "")), ...);
        ss << "\n";
    }
    return ss.str();
}

template<typename... Ts>
std::vector<std::tuple<Ts...>> generateRawData(size_t numberOfTuples) {
    std::vector<std::tuple<Ts...>> v;
    v.reserve(numberOfTuples);

    for (size_t i = 0; i < numberOfTuples; i++) {
        v.emplace_back(generateValue<Ts>()...);
    }

    return v;
}

template<typename T>
std::string as_string(T);

template<>
inline std::string as_string(std::string s) {
    return s;
}

template<typename T>
    requires requires(T t) { std::to_string(t); }
std::string as_string(T s) {
    return std::to_string(s);
}

#if __has_include(<simdjson.h> )
#include <simdjson.h>
template<typename... Ts>
simdjson::padded_string generateJSONData(size_t numberOfTuples) {
    std::stringstream ss;
    for (size_t i = 0; i < numberOfTuples; i++) {
        std::size_t n{0};
        ss << "{";
        ((ss << "\"" << static_cast<char>('a' + n) << "\":" << generateValue<Ts>() << (++n != sizeof...(Ts) ? "," : "")), ...);
        ss << "}\n";
    }
    auto asString = simdjson::padded_string(ss.str());
    assert(simdjson::validate_utf8(asString));
    return asString;
}

template<typename... Ts>
simdjson::padded_string generateJSONDataRandomOrder(size_t numberOfTuples) {

    std::stringstream ss;

    for (size_t i = 0; i < numberOfTuples; i++) {
        std::unordered_map<char, std::string> kv;
        std::size_t n{0};
        std::array<size_t, sizeof...(Ts)> index;
        std::iota(index.begin(), index.end(), 0);
        ((kv.emplace(static_cast<char>('a' + n++), as_string(generateValue<Ts>()))), ...);

        static std::random_device dev;
        static std::mt19937 rng(dev());
        static std::uniform_int_distribution<std::mt19937::result_type> dist(1, 6);

        auto perms = dist(rng);
        for (size_t j = 0; j < perms; j++) {
            std::next_permutation(index.begin(), index.end());
        }

        ss << "{";
        for (auto keyIndex : index) {
            ss << "\"" << static_cast<char>('a' + keyIndex) << "\":" << kv[static_cast<char>('a' + keyIndex)] << ",";
        }
        ss.seekp(-1, ss.cur);
        ss << "}\n";
    }

    auto asString = simdjson::padded_string(ss.str());
    assert(simdjson::validate_utf8(asString));
    return asString;
}
#endif

#endif//FORMATTEDDATAGENERATOR_HPP

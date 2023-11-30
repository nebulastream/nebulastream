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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_UTIL_FRAME_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_UTIL_FRAME_HPP_
#include <Exceptions/RuntimeException.hpp>
#include <unordered_map>
namespace NES::Nautilus {

/**
 * @brief A simple frame abstraction for code generation.
 * @tparam K key type
 * @tparam V value type
 */
template<class K, class V>
class Frame {
  public:
    V getValue(K key) {
        auto value = frameMap.find(key);
        if (value == frameMap.end()) {
            throw Exceptions::RuntimeException("Key " + key + " does not exists in frame.");
        }
        return value->second;
    }

    bool contains(K key) { return frameMap.contains(key); }

    void setValue(K key, V value) { frameMap.emplace(std::make_pair(key, value)); }
    std::unordered_map<K, V>& getContent() { return frameMap; }

  private:
    std::unordered_map<K, V> frameMap;
};

}// namespace NES::Nautilus

#endif  // NES_RUNTIME_INCLUDE_NAUTILUS_UTIL_FRAME_HPP_

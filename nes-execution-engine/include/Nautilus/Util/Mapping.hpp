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
#ifndef NES_NAUTILUS_UTIL_MAPPING_HPP_
#define NES_NAUTILUS_UTIL_MAPPING_HPP_
#include <algorithm>
#include <Util/Logger/Logger.hpp>
#include <vector>

namespace NES::Nautilus {


template<class K, class V>
class Mapping {
  public:
    V getValue(K key) {
        for (auto elem : mapping) {
            if (elem.first == key) {
                return elem.second;
            }
        }
        NES_THROW_RUNTIME_ERROR("Key " << key << " dose not exist.");
    }
    bool contains(K key) {
        for (auto elem : mapping) {
            if (elem.first == key) {
                return true;
            }
        }
        return false;
    }
    void setValue(K key, V value) { mapping.emplace_back(std::make_pair(key, value)); }
    void setUniqueValue(K key, V value) {
        if (!contains(key)) {
            mapping.emplace_back(std::make_pair(key, value));
        }
    }
    size_t size() { return mapping.size(); }
    std::vector<std::pair<K, V>>& getMapping() { return mapping; }
    std::pair<K, V>& operator[](size_t index) { return mapping[index]; }
    void clear() { mapping.clear(); }
    V& getLastValue() {
        return mapping.end()->second;
    }
    void remove(K key) {
        mapping.erase(std::remove(mapping.begin(), mapping.end(), std::make_pair(key, getValue(key))), mapping.end());
    }
    int getIndex(K key) {
        int i;
        auto it = find(mapping.begin(), mapping.end(), std::make_pair(key, getValue(key)));
        if (it != mapping.end()) {
            i = it - mapping.begin();
        }
        else {
            i = -1;
        }
        return i;
    }
  private:
    std::vector<std::pair<K, V>> mapping;
};

}// namespace NES::Nautilus

#endif//NES_NAUTILUS_UTIL_MAPPING_HPP_
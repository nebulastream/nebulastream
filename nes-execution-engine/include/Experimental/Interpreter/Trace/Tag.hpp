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

#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_TAG_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_TAG_HPP_

#include <functional>
#include <iostream>
#include <memory>
#include <ostream>
#include <unordered_map>
#include <variant>
#include <vector>

namespace NES::Experimental::Interpreter{

class Tag {
  public:
    bool operator==(const Tag& other) const { return other.addresses == addresses; }
    friend std::ostream& operator<<(std::ostream& os, const Tag& tag);
    std::vector<uint64_t> addresses;
};

struct TagHasher {
    std::size_t operator()(const Tag& k) const {
        using std::hash;
        using std::size_t;
        using std::string;
        auto hasher = std::hash<uint64_t>();
        std::size_t hashVal = 1;
        for (auto address : k.addresses) {
            hashVal = hashVal ^ hasher(address) << 1;
        }
        return hashVal;
    }
};

}


#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_TAG_HPP_

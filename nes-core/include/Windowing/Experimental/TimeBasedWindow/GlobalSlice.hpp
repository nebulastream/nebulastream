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

#ifndef NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_GLOBALSLICE_HPP_
#define NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_GLOBALSLICE_HPP_
#include <Util/Experimental/HashMap.hpp>
#include <cinttypes>
namespace NES::Windowing::Experimental {

class GlobalSlice {
  public:
    GlobalSlice(uint64_t start, uint64_t end, uint64_t index);

    GlobalSlice();

    inline uint64_t getStart() const { return start; }

    inline uint64_t getEnd() const { return end; }

    inline uint64_t getIndex() const { return index; }

    inline NES::Experimental::Hashmap& getState(){
        return state;
    }

    void reset(uint64_t start, uint64_t end, uint64_t index);


  private:
    uint64_t start;
    uint64_t end;
    uint64_t index;
    NES::Experimental::Hashmap state;
};

}// namespace NES::Windowing::Experimental

#endif//NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_GLOBALSLICE_HPP_

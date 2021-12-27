#ifndef NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_KEYEDSLICE_HPP_
#define NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_KEYEDSLICE_HPP_
#include <Windowing/Experimental/HashMap.hpp>
#include <cinttypes>
namespace NES::Windowing::Experimental {

class KeyedSlice {
  public:
    KeyedSlice(std::shared_ptr<NES::Experimental::HashMapFactory> hashMapFactory, uint64_t start, uint64_t end, uint64_t index);

    KeyedSlice(std::shared_ptr<NES::Experimental::HashMapFactory> hashMapFactory);

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

#endif//NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_KEYEDSLICE_HPP_

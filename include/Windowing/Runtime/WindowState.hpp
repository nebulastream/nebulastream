#ifndef NES_INCLUDE_WINDOWING_WINDOWSTATE_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWSTATE_HPP_

#include <cstdint>

namespace NES {

class WindowState {

  private:
    uint64_t start;
    uint64_t end;

  public:
    uint64_t getStartTs() const;
    uint64_t getEndTs() const;
    WindowState(uint64_t start, uint64_t end);
};

}// namespace NES

#endif//NES_INCLUDE_WINDOWING_WINDOWSTATE_HPP_

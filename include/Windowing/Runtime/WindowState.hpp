#ifndef NES_INCLUDE_WINDOWING_WINDOWSTATE_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWSTATE_HPP_

#include <cstdint>

namespace NES::Windowing {

/**
 * @brief Represents the start and end of a single window
 */
class WindowState {

  public:
    WindowState(uint64_t start, uint64_t end);

    /**
     * @brief Returns the window start.
     * @return uint64_t
     */
    uint64_t getStartTs() const;
    /**
    * @brief Returns the window end.
    * @return uint64_t
    */
    uint64_t getEndTs() const;

  private:
    uint64_t start;
    uint64_t end;
};

}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_WINDOWSTATE_HPP_

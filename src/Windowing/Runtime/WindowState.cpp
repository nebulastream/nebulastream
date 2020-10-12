
#include <Windowing/Runtime/WindowState.hpp>

namespace NES{

WindowState::WindowState(uint64_t start, uint64_t end) : start(start), end(end) {}

uint64_t WindowState::getStartTs() const {
    return start;
}
uint64_t WindowState::getEndTs() const {
    return end;
}


}
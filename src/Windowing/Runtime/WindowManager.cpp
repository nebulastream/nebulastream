#include <Windowing/Runtime/WindowManager.hpp>

namespace NES::Windowing {

WindowManager::WindowManager(LogicalWindowDefinitionPtr windowDefinition)
    : windowDefinition(std::move(windowDefinition)), allowedLateness(0) {}

LogicalWindowDefinitionPtr WindowManager::getWindowDefinition() {
    return windowDefinition;
}

uint64_t WindowManager::getAllowedLateness() const {
    return allowedLateness;
}

}// namespace NES::Windowing
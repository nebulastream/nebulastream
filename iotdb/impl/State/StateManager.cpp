#include "State/StateManager.hpp"

namespace NES {

StateManager& StateManager::instance() {
    static StateManager singleton;
    return singleton;
}
}// namespace NES
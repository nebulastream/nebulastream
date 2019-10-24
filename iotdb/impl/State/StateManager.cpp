#include "State/StateManager.hpp"

namespace iotdb {

StateManager& StateManager::instance() {
    static StateManager singleton;
    return singleton;
}
}
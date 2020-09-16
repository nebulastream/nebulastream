#ifndef NES_INCLUDE_NODEENGINE_RECONFIGURATIONTYPE_HPP_
#define NES_INCLUDE_NODEENGINE_RECONFIGURATIONTYPE_HPP_

#include <cstdint>

namespace NES {
enum ReconfigurationType : uint8_t {
    Initialize,
    Destroy,
    DestroyQep
};
}

#endif//NES_INCLUDE_NODEENGINE_RECONFIGURATIONTYPE_HPP_

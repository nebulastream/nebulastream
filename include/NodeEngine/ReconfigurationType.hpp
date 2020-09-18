#ifndef NES_INCLUDE_NODEENGINE_RECONFIGURATIONTYPE_HPP_
#define NES_INCLUDE_NODEENGINE_RECONFIGURATIONTYPE_HPP_

#include <cstdint>

namespace NES {
enum ReconfigurationType : uint8_t {
    /// use Initialize for reconfiguration tasks that initialize a reconfigurable instance
    Initialize,
    /// use Destroy for reconfiguration tasks that cleans up a reconfigurable instance
    Destroy,
};
}

#endif//NES_INCLUDE_NODEENGINE_RECONFIGURATIONTYPE_HPP_

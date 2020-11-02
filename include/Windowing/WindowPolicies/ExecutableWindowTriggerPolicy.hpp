#ifndef NES_INCLUDE_WINDOWING_WINDOWPOLICIES_EXECUTABLEWINDOWTRIGGERPOLICY_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWPOLICIES_EXECUTABLEWINDOWTRIGGERPOLICY_HPP_
#include <Windowing/WindowingForwardRefs.hpp>
namespace NES::Windowing {

class ExecutableWindowTriggerPolicy {
  public:
    /**
     * @brief This function stop the trigger policy
     * @return bool indicating success
     */
    virtual bool stop() = 0;

    /**
     * @brief This function starts the trigger policy
     * @return bool indicating success
     */
    virtual bool start(AbstractWindowHandlerPtr windowHandler) = 0;
};
}
#endif//NES_INCLUDE_WINDOWING_WINDOWPOLICIES_EXECUTABLEWINDOWTRIGGERPOLICY_HPP_

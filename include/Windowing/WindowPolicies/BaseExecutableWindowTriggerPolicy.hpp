#ifndef NES_INCLUDE_WINDOWING_WINDOWPOLICIES_BASEEXECUTABLEWINDOWTRIGGERPOLICY_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWPOLICIES_BASEEXECUTABLEWINDOWTRIGGERPOLICY_HPP_
#include <Windowing/WindowingForwardRefs.hpp>
namespace NES::Windowing {

class BaseExecutableWindowTriggerPolicy {
  public:
    /**
     * @brief This function starts the trigger policy
     * @return bool indicating success
     */
    virtual bool start(AbstractWindowHandlerPtr windowHandler) = 0;

    /**
    * @brief This function stop the trigger policy
    * @return bool indicating success
    */
    virtual bool stop() = 0;
};
}// namespace NES::Windowing
#endif//NES_INCLUDE_WINDOWING_WINDOWPOLICIES_BASEEXECUTABLEWINDOWTRIGGERPOLICY_HPP_

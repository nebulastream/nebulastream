#ifndef NES_INCLUDE_WINDOWING_WINDOWPOLICIES_EXECUTABLEONTIMETRIGGERPOLICY_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWPOLICIES_EXECUTABLEONTIMETRIGGERPOLICY_HPP_
#include <Windowing/WindowHandler/AbstractWindowHandler.hpp>
#include <Windowing/WindowPolicies/BaseExecutableWindowTriggerPolicy.hpp>
#include <memory>
#include <mutex>

namespace NES::Windowing {

class ExecutableOnTimeTriggerPolicy : public BaseExecutableWindowTriggerPolicy {
  public:
    ExecutableOnTimeTriggerPolicy(size_t triggerTimeInMs);

    static ExecutableOnTimeTriggerPtr create(size_t triggerTimeInMs);

    /**
     * @brief This function starts the trigger policy
     * @return bool indicating success
     */
    bool start(AbstractWindowHandlerPtr windowHandler) override;

    /**
     * @brief This function stop the trigger policy
     * @return bool indicating success
     */
    bool stop() override;

  private:
    bool running;
    std::shared_ptr<std::thread> thread;
    std::mutex runningTriggerMutex;
    size_t triggerTimeInMs;
};

}// namespace NES::Windowing
#endif//NES_INCLUDE_WINDOWING_WINDOWPOLICIES_EXECUTABLEONTIMETRIGGERPOLICY_HPP_

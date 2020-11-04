#ifndef NES_INCLUDE_WINDOWING_WINDOWPOLICIES_TIMETRIGGERDESCRIPTION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWPOLICIES_TIMETRIGGERDESCRIPTION_HPP_
#include <Windowing/WindowPolicies/BaseWindowTriggerPolicyDescriptor.hpp>

namespace NES::Windowing {

class OnTimeTriggerPolicyDescription : public BaseWindowTriggerPolicyDescriptor {
  public:
    static WindowTriggerPolicyPtr create(size_t triggerTimeInMs);

    /**
     * @brief method to get the policy type
     * @return
     */
    TriggerType getPolicyType() override;

    /**
    * @brief getter for the time in ms
    * @return time in ms between two triggers
    */
    size_t getTriggerTimeInMs() const;

  protected:
    OnTimeTriggerPolicyDescription(size_t triggerTimeInMs);
    size_t triggerTimeInMs;
};

typedef std::shared_ptr<OnTimeTriggerPolicyDescription> OnTimeTriggerDescriptionPtr;
}// namespace NES::Windowing
#endif//NES_INCLUDE_WINDOWING_WINDOWPOLICIES_TIMETRIGGERDESCRIPTION_HPP_

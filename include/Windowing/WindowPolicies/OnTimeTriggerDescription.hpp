#ifndef NES_INCLUDE_WINDOWING_WINDOWPOLICIES_TIMETRIGGERDESCRIPTION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWPOLICIES_TIMETRIGGERDESCRIPTION_HPP_
#include <Windowing/WindowPolicies/WindowTriggerPolicyDescriptor.hpp>

namespace NES::Windowing {

class OnTimeTriggerDescription : public WindowTriggerPolicyDescriptor {
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
    OnTimeTriggerDescription(size_t triggerTimeInMs);
    size_t triggerTimeInMs;
};

typedef std::shared_ptr<OnTimeTriggerDescription> OnTimeTriggerDescriptionPtr;
}
#endif//NES_INCLUDE_WINDOWING_WINDOWPOLICIES_TIMETRIGGERDESCRIPTION_HPP_

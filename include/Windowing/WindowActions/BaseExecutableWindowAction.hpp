#ifndef NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLEWINDOWACTION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLEWINDOWACTION_HPP_
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Windowing {

template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
class BaseExecutableWindowAction {
  public:
    /**
     * @brief This function does the action
     * @return bool indicating success
     */
    virtual bool doAction(StateVariable<KeyType, WindowSliceStore<PartialAggregateType>*>* windowStateVariable, TupleBuffer& tupleBuffer) = 0;

    virtual std::string toString() = 0;

    virtual SchemaPtr getWindowSchema() = 0;
};
}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLEWINDOWACTION_HPP_

#ifndef NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLEWINDOWACTION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLEWINDOWACTION_HPP_

namespace NES::Windowing {

class BaseExecutableWindowAction {
  public:
    /**
     * @brief This function does the action
     * @return bool indicating success
     */
    virtual bool doAction() = 0;
};
}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLEWINDOWACTION_HPP_

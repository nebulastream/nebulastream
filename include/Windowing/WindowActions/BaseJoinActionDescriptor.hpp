#ifndef NES_INCLUDE_WINDOWING_WINDOWACTIONS_JOINACTIONDESCRIPTOR_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWACTIONS_JOINACTIONDESCRIPTOR_HPP_
#include <Windowing/JoinForwardRefs.hpp>

namespace NES::Join {

enum JoinActionType {
    LazyNestedLoopJoin
};

class BaseJoinActionDescriptor {
  public:
    /**
     * @brief this function will return the type of the action
     * @return
     */
    virtual JoinActionType getActionType() = 0;

  protected:
    BaseJoinActionDescriptor(JoinActionType action);
    JoinActionType action;
};

}// namespace NES::Windowing
#endif//NES_INCLUDE_WINDOWING_WINDOWACTIONS_JOINACTIONDESCRIPTOR_HPP_

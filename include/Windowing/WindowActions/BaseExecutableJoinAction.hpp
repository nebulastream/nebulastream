#ifndef NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLEJOIN_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLEJOIN_HPP_
#include <Windowing/WindowingForwardRefs.hpp>
#include <State/StateVariable.hpp>

namespace NES::Join {

template<class KeyType>
class BaseExecutableJoinAction {
  public:
    /**
     * @brief This function does the action
     * @return bool indicating success
     */
    virtual bool doAction(StateVariable<KeyType, Windowing::WindowSliceStore<KeyType>*>* leftJoinState,
                          StateVariable<KeyType, Windowing::WindowSliceStore<KeyType>*>* rightJoinSate,
                          TupleBuffer& tupleBuffer) = 0;

    virtual std::string toString() = 0;

    virtual SchemaPtr getJoinSchema() = 0;
};
}// namespace NES::Join

#endif//NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLEJOIN_HPP_

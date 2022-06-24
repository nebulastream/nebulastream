#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_NESIR_FRAME_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_NESIR_FRAME_HPP_
#include <unordered_map>
namespace NES::ExecutionEngine::Experimental::IR {

template<class K, class V>
class Frame {
  public:
    V getValue(K key) {
        auto value = frameMap.find(key);
        if (value == frameMap.end()) {
        }
        return value->second;
    }

    void setValue(K key, V value) { frameMap[key] = value; }

  private:
    std::unordered_map<K, V> frameMap;
};

}// namespace NES::ExecutionEngine::Experimental::IR

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_NESIR_FRAME_HPP_

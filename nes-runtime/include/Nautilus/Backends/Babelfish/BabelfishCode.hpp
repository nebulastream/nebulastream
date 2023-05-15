#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BABELFISH_BABELFISHCODE_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BABELFISH_BABELFISHCODE_HPP_

#include "Nautilus/IR/Types/Stamp.hpp"
#include <Compiler/DynamicObject.hpp>
#include <Nautilus/Backends/Executable.hpp>
#include <jni.h>
#include <memory>
#include <vector>
namespace NES::Nautilus::Backends::Babelfish {

enum Type { v, i8, i16, i32, i64, d, f };
class BabelfishCode {
  public:
    std::string code;
    IR::Types::StampPtr resultType;
    std::vector<Type> argTypes;
};

}// namespace NES::Nautilus::Backends::Babelfish

#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BABELFISH_BABELFISHCODE_HPP_

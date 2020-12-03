#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_RUNTIME_SHAREDPOINTERGENERATION_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_RUNTIME_SHAREDPOINTERGENERATION_HPP_
#include <QueryCompiler/CCodeGenerator/CCodeGeneratorForwardRef.hpp>
namespace NES{

class SharedPointerGen{
  public:
    GeneratableDataTypePtr static createSharedPtrType(GeneratableDataTypePtr type);
    StatementPtr static makeShared(GeneratableDataTypePtr type);
};

}

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_RUNTIME_SHAREDPOINTERGENERATION_HPP_

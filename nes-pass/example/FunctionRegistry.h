#ifndef FUNCTIONREGISTRY_H
#define FUNCTIONREGISTRY_H
#include <unordered_map>
#include <mutex>
#include <string>

class FunctionRegistry {
public:
    using FuncPtr = void(*)();

    static FunctionRegistry& instance();

    int addToRegistry(FuncPtr fn, std::string bitcode);
    std::string getMetadata(FuncPtr fn) const;

private:
    mutable std::mutex mutex_;
    std::unordered_map<FuncPtr, std::string> registry_;
};


int registerFunction(FunctionRegistry::FuncPtr fn, const char *bitcodePtr, uint64_t bitcodeLen);


//This needs to exist here, in order to inject the otherwise unused registerFunction declaration into the LLVM IR
__attribute__((used)) static const auto registrationFuncPtrDummyPleaseIgnoreThisThanks = &registerFunction;


#define NAUT_INLINE __attribute__((annotate("naut_inline")))

#endif

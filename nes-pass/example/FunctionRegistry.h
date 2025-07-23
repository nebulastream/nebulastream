#ifndef FUNCTIONREGISTRY_H
#define FUNCTIONREGISTRY_H
#include <unordered_map>
#include <mutex>
#include <string>

class InlineFunctionRegistry {
public:
    static InlineFunctionRegistry& instance();

    int addToRegistry(void *fn, std::string bitcode);
    std::string getMetadata(void *fn) const;

private:
    mutable std::mutex mutex_;
    std::unordered_map<void *, std::string> registry_;
};


int registerFunction(void *fn, const char *bitcodePtr, uint64_t bitcodeLen);


//This needs to exist here, in order to inject the otherwise unused registerFunction declaration into the LLVM IR
__attribute__((used)) static const auto registrationFuncPtrDummyPleaseIgnoreThisThanks = &registerFunction;


#define NAUT_INLINE __attribute__((annotate("naut_inline")))

#endif

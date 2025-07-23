#ifndef NAUTLILUS_H
#define NAUTLILUS_H
#include <iostream>
#include "FunctionRegistry.h"


template <typename Func>
void invoke(Func func) {
    (void)func;
    if (InlineFunctionRegistry::instance().getMetadata((void *) func).size() > 0)
    {
        std::cout << "Found function in registry; inlining\n";
        auto bitcode = InlineFunctionRegistry::instance().getMetadata((void *) func);
        std::cout << bitcode << std::endl;
    } else
    {
        std::cout << "Function not in registry; inserting normal call\n";
    }
}

#endif

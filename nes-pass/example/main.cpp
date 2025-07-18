#include <ostream>
#include <iostream>

#include "FunctionRegistry.h"
#include "nautlilus.h"
#include "lib.h"


NAUT_INLINE void target()
{
    std::cout << "Hello World!\n";
}



int main()
{
    invoke(target);
    invoke(libTargetInline);
    invoke(libTargetNoInline);
}



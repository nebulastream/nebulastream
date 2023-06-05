import subprocess
from timeit import default_timer as timer
def doTest(size):
    with open(str(size) + "_test.cpp", "w") as out:
        print("#include <Nautilus/Interface/DataTypes/Value.hpp>", file=out)
        print("#include <Nautilus/Interface/FunctionCall.hpp>", file=out)
        print("namespace NES::Nautilus {", file=out)
        print("int32_t predicate(int32_t x, int32_t y);", file=out)

        print("Value<> test_"+str(size)+"(Value<Int32> value, Value<Int32> value2) {", file=out)
        for s in range(size):
            p="x" if s==0 else f'l{s-1}'
            print (f'if (FunctionCall("predicate", predicate, value, value2) == value2)', file=out)
        print(f'return value + 42;\n    return value;}}', file=out)
        print('}', file=out)

for size in [1, 10, 100, 500, 1000]:
    doTest(size)
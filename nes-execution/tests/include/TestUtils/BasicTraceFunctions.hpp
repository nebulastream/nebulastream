/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_EXECUTION_TESTS_INCLUDE_TESTUTILS_BASICTRACEFUNCTIONS_HPP_
#define NES_EXECUTION_TESTS_INCLUDE_TESTUTILS_BASICTRACEFUNCTIONS_HPP_

#include <Nautilus/Interface/DataTypes/Value.hpp>

namespace NES::Nautilus::Tracing {

void assignmentOperator() {
    auto iw = Value<>(1);
    auto iw2 = Value<>(2);
    iw = iw2 + iw;
}

void arithmeticExpression() {
    Value iw = Value(1);
    Value iw2 = Value(2);
    Value iw3 = Value(3);
    auto result = iw - iw3 + 2 * iw2 / iw;
}

void logicalNegate() {
    Value iw = Value(true);
    auto result = !iw;
}

void logicalExpressionLessThan() {
    Value iw = Value(1);
    auto result = iw < 2;
}

void logicalExpressionEquals() {
    Value iw = Value(1);
    auto result = iw == 2;
}

void logicalExpressionLessEquals() {
    Value iw = Value(1);
    auto result = iw <= 2;
}

void logicalExpressionGreater() {
    Value iw = Value(1);
    auto result = iw > 2;
}

void logicalExpressionGreaterEquals() {
    Value iw = Value(1);
    auto result = iw >= 2;
}

Value<> logicalAssignTest() {
    Value<Boolean> res = true;
    Value x = Value(1);
    res = res && x == 42;
    Value y = Value(1);
    res = res && y == 42;
    Value z = Value(1);
    return res && z == 42;
}

void logicalExpression() {
    Value iw = Value(1);
    auto result = iw == 2 && iw < 1 || true;
}

void ifCondition(bool flag) {
    Value boolFlag = Value(flag);
    Value iw = Value(1);
    if (boolFlag) {
        iw = iw - 1;
    }
    auto x = iw + 42;
}

void ifElseCondition(bool flag) {
    Value boolFlag = Value(flag);
    Value iw = Value(1);
    Value iw2 = Value(1);
    if (boolFlag) {
        iw = iw - 1;
    } else {
        iw = iw * iw2;
    }
    iw + 1;
}

void nestedIfThenElseCondition() {
    Value value = Value(1);
    Value iw = Value(1);
    if (value == 42) {
    } else {
        if (iw == 8) {
        } else {
            iw = iw + 2;
        }
    }
}

void emptyLoop() {
    Value iw = Value(1);
    Value iw2 = Value(2);
    for (auto start = iw; start < 2; start = start + 1) {
    }
    auto iw3 = iw2 - 5;
}

void longEmptyLoop() {
    Value iw = Value(1);
    Value iw2 = Value(2);
    for (auto start = iw; start < 20000; start = start + 1) {
    }
    auto iw3 = iw2 - 5;
}

void sumLoop() {
    Value agg = Value(1);
    for (auto start = Value(0); start < 10; start = start + 1) {
        agg = agg + 1;
    }
    auto res = agg == 10;
}

void sumWhileLoop() {
    Value agg = Value(1);
    while (agg < 20) {
        agg = agg + 1;
    }
    auto res = agg == 10;
}

void invertedLoop() {
    Value i = Value(0);
    Value end = Value(300);
    do {
        // body
        i = i + 1;
    } while (i < end);
}

void nestedLoop() {
    Value agg = Value(0);
    Value i = Value(0);
    Value end = Value(300);
    for (; i < end; i = i + 1) {
        for (Value j = 0; j < 10; j = j + 1) {
            agg = agg + 1;
        }
    }
}

void nestedLoopIf() {
    Value agg = Value(0);
    Value i = Value(0);
    Value end = Value(300);
    for (; i < end; i = i + 1) {
        for (Value j = 0; j < 10; j = j + 1) {
            if (agg < 50)
                agg = agg + 1;
            else {
                agg = agg / 2;
            }
        }
    }
}

void loopWithBreak() {
    Value agg = Value(0);
    Value i = Value(0);
    Value end = Value(300);
    for (; i < end; i = i + 1) {
        if (agg == 42) {
            break;
        }
        agg = agg + 1;
    }
}

Value<> f2(Value<> x) {
    if (x > 10)
        return x;
    else
        return x + 1;
}

Value<> nativeLoop() {
    Value agg = Value(0);
    for (uint64_t i = 0; i < 10; i++) {
        if (agg > 42) {
            agg = agg + 1;
        }
    }
    return agg;
}

void f1() {
    Value agg = Value(0);
    agg = f2(agg) + 42;
}

void deepLoop() {
    Value i = Value(0);
    while (i < 10) {
        while (i < 10) {
            while (i < 10) {
                while (i < 10) {
                    while (i < 10) {
                        while (i < 10) {
                            while (i < 10) {
                                while (i < 10) {
                                    while (i < 10) {
                                        while (i < 10) {
                                            i = i + 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    while (i < 10) {
        while (i < 10) {
            while (i < 10) {
                while (i < 10) {
                    while (i < 10) {
                        while (i < 10) {
                            while (i < 10) {
                                while (i < 10) {
                                    while (i < 10) {
                                        while (i < 10) {
                                            i = i + 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

Value<> TracingBreaker() {
    Value agg = Value(0);
    Value limit = Value(10);
    if (agg < 350) {
        agg = agg + 1;
    }
    if (agg < 350) {
        agg = agg + 1;
    } else {
        if (agg < 350) {
            if (agg < 350) {//the 'false' case of this if this if-operation has no operations -> Block_9
                            // agg = agg + 1;
            } else {
                //    agg = agg + 2;// leads to empty block
            }
        }
    }
    return agg;
}

}// namespace NES::Nautilus::Tracing
#endif // NES_EXECUTION_TESTS_INCLUDE_TESTUTILS_BASICTRACEFUNCTIONS_HPP_

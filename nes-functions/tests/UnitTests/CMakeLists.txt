# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_nes_test_functions(arithmetical-functions-test
        "ArithmeticalFunctions/NodeFunctionAbsTest.cpp"
        "ArithmeticalFunctions/NodeFunctionAddTest.cpp"
        "ArithmeticalFunctions/NodeFunctionCeilTest.cpp"
        "ArithmeticalFunctions/NodeFunctionDivTest.cpp"
        "ArithmeticalFunctions/NodeFunctionExpTest.cpp"
        "ArithmeticalFunctions/NodeFunctionFloorTest.cpp"
        "ArithmeticalFunctions/NodeFunctionModTest.cpp"
        "ArithmeticalFunctions/NodeFunctionMulTest.cpp"
        "ArithmeticalFunctions/NodeFunctionPowTest.cpp"
        "ArithmeticalFunctions/NodeFunctionRoundTest.cpp"
        "ArithmeticalFunctions/NodeFunctionSqrtTest.cpp"
        "ArithmeticalFunctions/NodeFunctionSubTest.cpp"
)

add_nes_test_functions(logical-functions-test
        "LogicalFunctions/NodeFunctionAndTest.cpp"
        "LogicalFunctions/NodeFunctionEqualsTest.cpp"
        "LogicalFunctions/NodeFunctionGreaterEqualsTest.cpp"
        "LogicalFunctions/NodeFunctionGreaterTest.cpp"
        "LogicalFunctions/NodeFunctionLessEqualsTest.cpp"
        "LogicalFunctions/NodeFunctionLessTest.cpp"
        "LogicalFunctions/NodeFunctionNegateTest.cpp"
        "LogicalFunctions/NodeFunctionOrTest.cpp"
)

add_nes_test_functions(leaf-functions-test
        "NodeFunctionConstantValueTest.cpp"
        "NodeFunctionFieldAccessTest.cpp"
        "NodeFunctionFieldAssignmentTest.cpp"
)

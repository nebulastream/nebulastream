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
        "ArithmeticalFunctions/AbsFunctionNodeTest.cpp"
        "ArithmeticalFunctions/AddFunctionNodeTest.cpp"
        "ArithmeticalFunctions/CeilFunctionNodeTest.cpp"
        "ArithmeticalFunctions/DivFunctionNodeTest.cpp"
        "ArithmeticalFunctions/ExpFunctionNodeTest.cpp"
        "ArithmeticalFunctions/FloorFunctionNodeTest.cpp"
        "ArithmeticalFunctions/ModFunctionNodeTest.cpp"
        "ArithmeticalFunctions/MulFunctionNodeTest.cpp"
        "ArithmeticalFunctions/PowFunctionNodeTest.cpp"
        "ArithmeticalFunctions/RoundFunctionNodeTest.cpp"
        "ArithmeticalFunctions/SqrtFunctionNodeTest.cpp"
        "ArithmeticalFunctions/SubFunctionNodeTest.cpp"
)

add_nes_test_functions(logical-functions-test
        "LogicalFunctions/AndFunctionNodeTest.cpp"
        "LogicalFunctions/EqualsFunctionNodeTest.cpp"
        "LogicalFunctions/GreaterEqualsFunctionNodeTest.cpp"
        "LogicalFunctions/GreaterFunctionNodeTest.cpp"
        "LogicalFunctions/LessEqualsFunctionNodeTest.cpp"
        "LogicalFunctions/LessFunctionNodeTest.cpp"
        "LogicalFunctions/NegateFunctionNodeTest.cpp"
        "LogicalFunctions/OrFunctionNodeTest.cpp"
)

add_nes_test_functions(leaf-functions-test
        "ConstantValueFunctionNodeTest.cpp"
        "FieldAccessFunctionNodeTest.cpp"
        "FieldAssignmentFunctionNodeTest.cpp"
)

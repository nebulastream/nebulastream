# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

### LogicalExpressionTest ###
add_nes_unit_test(logical-expression-test "Query/LogicalExpressionTest.cpp")

### LogicalOperatorTest ###
add_nes_unit_test(logical-operator-node-tests
        "Operators/LogicalOperatorTest.cpp")

# ExpressionNodeTest
add_nes_unit_test(expression-node-test "Operators/ExpressionNodeTest.cpp")

# OperatorPropertiesTest
add_nes_unit_test(operator-properties-tests "Operators/OperatorPropertiesTest.cpp")

# SerializationUtilTest
add_nes_unit_test(serialize-operator-test "Serialization/SerializationUtilTest.cpp" "Serialization/TimeWindowSerializationTest.cpp")

### SourceCatalogService Tests ###
add_nes_unit_test(source-catalog-service-tests "Services/SourceCatalogServiceTest.cpp")

add_subdirectory(Catalogs)

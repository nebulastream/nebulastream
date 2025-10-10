# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

vcpkg_from_github(
        OUT_SOURCE_PATH SOURCE_PATH
        REPO google/or-tools
        REF 93dafb79a0261b5211f4eb17437aeebd7f45e543
        SHA512 8891c0987ea18ea381cc1ad99dbe8db89d8f6f0a2f952dd7f3b3bbcc5783d657e24d6573df851945408b77d82e89457f94acafb4bf1b42b58cd49fa076dca90a
        PATCHES
        0001-remove-gurobi.patch
)

vcpkg_cmake_configure(
        SOURCE_PATH "${SOURCE_PATH}"
        OPTIONS
        -DBUILD_SAMPLES=OFF
        -DBUILD_EXAMPLES=OFF
        -DBUILD_TESTING=OFF
        -DUSE_SCIP=OFF
        -DUSE_GUROBI=OFF
        -DUSE_HIGHS=OFF
        -DUSE_COINOR=OFF
)


vcpkg_cmake_install()
vcpkg_copy_pdbs()
vcpkg_copy_tools(TOOL_NAMES solve AUTO_CLEAN)
vcpkg_copy_tools(TOOL_NAMES fzn-cp-sat AUTO_CLEAN)
vcpkg_cmake_config_fixup(CONFIG_PATH "lib/cmake/ortools")

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/share")

# removes empty dirs
file(REMOVE_RECURSE
        "${CURRENT_PACKAGES_DIR}/include/ortools/algorithms/csharp"
        "${CURRENT_PACKAGES_DIR}/include/ortools/algorithms/java"
        "${CURRENT_PACKAGES_DIR}/include/ortools/algorithms/samples"
        "${CURRENT_PACKAGES_DIR}/include/ortools/constraint_solver/csharp"
        "${CURRENT_PACKAGES_DIR}/include/ortools/constraint_solver/docs"
        "${CURRENT_PACKAGES_DIR}/include/ortools/constraint_solver/python"
        "${CURRENT_PACKAGES_DIR}/include/ortools/constraint_solver/samples"
        "${CURRENT_PACKAGES_DIR}/include/ortools/cpp"
        "${CURRENT_PACKAGES_DIR}/include/ortools/dotnet"
        "${CURRENT_PACKAGES_DIR}/include/ortools/flatzinc/challenge"
        "${CURRENT_PACKAGES_DIR}/include/ortools/flatzinc/mznlib"
        "${CURRENT_PACKAGES_DIR}/include/ortools/glop/samples"
        "${CURRENT_PACKAGES_DIR}/include/ortools/graph/csharp"
        "${CURRENT_PACKAGES_DIR}/include/ortools/graph/java"
        "${CURRENT_PACKAGES_DIR}/include/ortools/graph/python"
        "${CURRENT_PACKAGES_DIR}/include/ortools/graph/samples"
        "${CURRENT_PACKAGES_DIR}/include/ortools/graph/testdata"
        "${CURRENT_PACKAGES_DIR}/include/ortools/init/csharp"
        "${CURRENT_PACKAGES_DIR}/include/ortools/init/java"
        "${CURRENT_PACKAGES_DIR}/include/ortools/java"
        "${CURRENT_PACKAGES_DIR}/include/ortools/julia"
        "${CURRENT_PACKAGES_DIR}/include/ortools/linear_solver/csharp"
        "${CURRENT_PACKAGES_DIR}/include/ortools/linear_solver/java"
        "${CURRENT_PACKAGES_DIR}/include/ortools/linear_solver/python"
        "${CURRENT_PACKAGES_DIR}/include/ortools/linear_solver/testdata"
        "${CURRENT_PACKAGES_DIR}/include/ortools/math_opt/core/python"
        "${CURRENT_PACKAGES_DIR}/include/ortools/math_opt/elemental/python"
        "${CURRENT_PACKAGES_DIR}/include/ortools/math_opt/io/python"
        "${CURRENT_PACKAGES_DIR}/include/ortools/math_opt/python"
        "${CURRENT_PACKAGES_DIR}/include/ortools/math_opt/samples"
        "${CURRENT_PACKAGES_DIR}/include/ortools/math_opt/solver_tests/testdata"
        "${CURRENT_PACKAGES_DIR}/include/ortools/packing/testdata"
        "${CURRENT_PACKAGES_DIR}/include/ortools/pdlp/python"
        "${CURRENT_PACKAGES_DIR}/include/ortools/pdlp/samples"
        "${CURRENT_PACKAGES_DIR}/include/ortools/python"
        "${CURRENT_PACKAGES_DIR}/include/ortools/routing/parsers/testdata"
        "${CURRENT_PACKAGES_DIR}/include/ortools/routing/samples"
        "${CURRENT_PACKAGES_DIR}/include/ortools/sat/colab"
        "${CURRENT_PACKAGES_DIR}/include/ortools/sat/csharp"
        "${CURRENT_PACKAGES_DIR}/include/ortools/sat/docs"
        "${CURRENT_PACKAGES_DIR}/include/ortools/sat/fuzz_testdata"
        "${CURRENT_PACKAGES_DIR}/include/ortools/sat/go"
        "${CURRENT_PACKAGES_DIR}/include/ortools/sat/java"
        "${CURRENT_PACKAGES_DIR}/include/ortools/sat/samples"
        "${CURRENT_PACKAGES_DIR}/include/ortools/scheduling/python"
        "${CURRENT_PACKAGES_DIR}/include/ortools/scheduling/testdata"
        "${CURRENT_PACKAGES_DIR}/include/ortools/service/"
        "${CURRENT_PACKAGES_DIR}/include/ortools/set_cover/python"
        "${CURRENT_PACKAGES_DIR}/include/ortools/set_cover/samples"
        "${CURRENT_PACKAGES_DIR}/include/ortools/util/csharp"
        "${CURRENT_PACKAGES_DIR}/include/ortools/util/java"
)

file(INSTALL ${SOURCE_PATH}/LICENSE DESTINATION ${CURRENT_PACKAGES_DIR}/share/${PORT} RENAME copyright)

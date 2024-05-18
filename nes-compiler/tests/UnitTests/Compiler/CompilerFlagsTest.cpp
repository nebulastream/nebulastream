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

#include <BaseUnitTest.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/CompilerFlags.hpp>
class CompilerFlagsTest : public NES::Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("CompilerFlagsTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup CompilationCacheTest test class.");
    }
};

TEST_F(CompilerFlagsTest, TestCopyConstruction) {
    NES::Compiler::CompilerFlags flags;
    flags.addFlag("Yeee");
    flags.addFlag("Yeeeee");

    NES::Compiler::CompilerFlags flags2(flags);
    flags2.addFlag("Yeee");
    flags2.addFlag("Yeee44");

    EXPECT_THAT(flags.getFlags(), ::testing::ElementsAre("Yeee", "Yeeeee"));
    EXPECT_THAT(flags2.getFlags(), ::testing::ElementsAre("Yeee", "Yeeeee", "Yeee44"));
}

TEST_F(CompilerFlagsTest, TestCopyAssignmentWithDeletionOfOriginal) {
    NES::Compiler::CompilerFlags flags2;

    {
        NES::Compiler::CompilerFlags flags;
        flags.addFlag("Yeee");
        flags.addFlag("Yeeeee");
        flags2 = flags;
    }

    flags2.addFlag("Yeee");
    flags2.addFlag("Yeee44");

    EXPECT_THAT(flags2.getFlags(), ::testing::ElementsAre("Yeee", "Yeeeee", "Yeee44"));
}

TEST_F(CompilerFlagsTest, TestMoveConstruction) {
    NES::Compiler::CompilerFlags flags;
    flags.addFlag("Yeee");
    flags.addFlag("Yeeeee");

    NES::Compiler::CompilerFlags flags2(std::move(flags));
    flags2.addFlag("Yeee");
    flags2.addFlag("Yeee44");

    EXPECT_THAT(flags2.getFlags(), ::testing::ElementsAre("Yeee", "Yeeeee", "Yeee44"));
}

TEST_F(CompilerFlagsTest, TestMerging) {
    NES::Compiler::CompilerFlags flags;
    flags.addFlag("Yeee");
    flags.addFlag("Yeeeee");

    NES::Compiler::CompilerFlags flags2;
    flags2.addFlag("Yeee");
    flags2.addFlag("Yeee44");

    flags2.mergeFlags(std::move(flags));

    EXPECT_THAT(flags2.getFlags(), ::testing::ElementsAre("Yeee", "Yeee44", "Yeeeee"));
}

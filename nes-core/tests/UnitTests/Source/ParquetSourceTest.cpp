//
// Created by balint on 04.07.22.
//

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
#ifdef ENABLE_PARQUET_BUILD
#include <NesBaseTest.hpp>
#include <gtest/gtest.h>
#include <Sources/ParquetSource.hpp>
#include <Sources/ParquetStaticVariables.hpp>
#include <Util/Logger/Logger.hpp>
#include <arrow/io/file.h>
#include <parquet/stream_reader.h>
namespace NES {
class ParquetSourceTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup ParquetSource test class." << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::Logger::setupLogging("ParquetSourceTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup ParquetSourceTest test case." << std::endl;
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down ParquetSourceTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down ParquetSourceTest test class." << std::endl; }

};

TEST_F(ParquetSourceTest, proofOfConcept) {
    std::shared_ptr<arrow::io::ReadableFile> infile;

    PARQUET_ASSIGN_OR_THROW(
        infile,
        arrow::io::ReadableFile::Open("/home/balint/Documents/Uni/Masters/2_SEM/BDSPRO/MockSingle.parquet"));

    auto descriptor= infile->file_descriptor();

    parquet::StreamReader reader = parquet::StreamReader(parquet::ParquetFileReader::Open(infile));
    reader >> uint16 >> doubleType >> string >> boolean >> uint32 >> parquet::EndRow;

     std::cout << uint16 << doubleType << string << boolean << uint32;
}
}
#endif
#include <API/Schema.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Experimental/Utility/TestUtility.hpp>
#include <fstream>
#include <ios>
#include <memory>
#include <numeric>
#include <string>
#include <type_traits>

namespace NES::ExecutionEngine::Experimental {

const std::unique_ptr<TestUtility::TestParameterConfig> TestUtility::getTestParamaterConfig(const std::string &resultsFileName) {
    // Create standard test paramater config and return.
    return std::make_unique<TestUtility::TestParameterConfig>(
        TestUtility::TestParameterConfig {
            NES::Nautilus::Backends::MLIR::LLVMIROptimizer::O3,
            false,
            10,
            9,
            resultsFileName,
            std::vector<std::string> {
                "Symbolic Execution Trace     ",
                "SSA Phase                    ",
                "IR Created                   ",
                "MLIR Generation              ",
                "MLIR Optimization            ",
                "MLIR Lowering                ",
                "LLVM JIT Compilation         ",
                "Executed                     ",
                "Overall Time                 "
            }
        }
    );
}

std::pair<std::shared_ptr<NES::Runtime::MemoryLayouts::RowLayout>, NES::Runtime::MemoryLayouts::DynamicTupleBuffer> 
        NES::ExecutionEngine::Experimental::TestUtility::loadLineItemTable(std::shared_ptr<Runtime::BufferManager> bm) {
    std::ifstream inFile("/home/alepping/tpch/dbgen/lineitem.tbl");
    uint64_t linecount = 0;
    std::string line;
    while (std::getline(inFile, line)) {
        // using printf() in all tests for consistency
        linecount++;
    }
    NES_DEBUG("LOAD lineitem with " << linecount << " lines");
    // Todo try columnar layout
//    auto schema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);

    // orderkey
    // partkey
    // suppkey
    // linenumber
    schema->addField("l_quantity", BasicType::INT64);
    schema->addField("l_extendedprice", BasicType::INT64);
    schema->addField("l_discount", BasicType::INT64);
    // tax
    // returnflag
    // linestatus
    schema->addField("l_shipdate", BasicType::INT64);
    // commitdate
    // receiptdate
    // shipinstruct
    // shipmode
    // comment
    auto targetBufferSize = schema->getSchemaSizeInBytes() * linecount;
    auto buffer = bm->getUnpooledBuffer(targetBufferSize).value();
    // Todo column layout
//    auto memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, buffer.getBufferSize());
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, buffer.getBufferSize());
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);

    inFile.clear();// clear fail and eof bits
    inFile.seekg(0, std::ios::beg);

    int currentLineCount = 0;
    // 6001215
    // 2715000
    printf("Current LineCount: %ld\n", linecount);
    while (std::getline(inFile, line)) {
        if(!(currentLineCount % 60000)) {
            printf("Current LineCount: %f\n", currentLineCount/60012.150);
        }
        ++currentLineCount;
        // using printf() in all tests for consistency
        auto index = dynamicBuffer.getNumberOfTuples();
        auto strings = NES::Util::splitWithStringDelimiter<std::string>(line, "|");

        auto l_quantityString = strings[4];
        int64_t l_quantity = std::stoi(l_quantityString);
        dynamicBuffer[index][0].write(l_quantity);

        auto l_extendedpriceString = strings[5];
        int64_t l_extendedprice = std::stof(l_extendedpriceString) * 100;
        dynamicBuffer[index][1].write(l_extendedprice);

        auto l_discountString = strings[6];
        int64_t l_discount = std::stof(l_discountString) * 100;
        dynamicBuffer[index][2].write(l_discount);

        auto l_shipdateString = strings[10];
        NES::Util::findAndReplaceAll(l_shipdateString, "-", "");
        int64_t l_shipdate = std::stoi(l_shipdateString);
        dynamicBuffer[index][3].write(l_shipdate);
        dynamicBuffer.setNumberOfTuples(index + 1);
    }
    inFile.close();
    NES_DEBUG("Loading of Lineitem done");
    return std::make_pair(memoryLayout, dynamicBuffer);
}

std::vector<std::string> NES::ExecutionEngine::Experimental::TestUtility::loadStringsFromLineitemTable() {
    std::ifstream inFile("/home/alepping/tpch/dbgen/lineitem.tbl");
    uint64_t linecount = 0;
    std::string line;
    while (std::getline(inFile, line)) {
        // using printf() in all tests for consistency
        linecount++;
    }
    NES_DEBUG("LOAD lineitem with " << linecount << " lines");

    inFile.clear();// clear fail and eof bits
    inFile.seekg(0, std::ios::beg);

    int currentLineCount = 0;
    std::vector<std::string> lineitemStrings;
    while (std::getline(inFile, line)) {
        if(!(currentLineCount % 60000)) {
            printf("Current LineCount: %f\n", currentLineCount/60012.150);
        }
        //Comments 44 bytes
        auto strings = NES::Util::splitWithStringDelimiter<std::string>(line, "|");
        lineitemStrings.emplace_back(strings[15]);
        ++currentLineCount;
    }
    inFile.close();
    NES_DEBUG("Loading of Lineitem done");
    return lineitemStrings;
}


void NES::ExecutionEngine::Experimental::TestUtility::produceResults(std::vector<std::vector<double>> runningSnapshotVectors, 
                                                    std::vector<std::string> snapshotNames, 
                                                    const std::string &resultsFileName, bool writeRawData,
                                                    const int NUM_ITERATIONS) {
    std::ifstream inlineFS("llvmLambda.csv");

    if(inlineFS.is_open()) {
        std::string line;
        std::vector<double> llvmParsingTimes;
        std::vector<double> llvmIRLinkingTimes;
        std::vector<double> llvmIROptimizationTimes;

        int numProcessedLines = 0;
        while(std::getline(inlineFS, line)) {
            if(resultsFileName == "tpch-q3-p1.csv" && numProcessedLines >= NUM_ITERATIONS) { 
                break;
            }
            if(resultsFileName == "tpch-q3-p2.csv" && numProcessedLines < NUM_ITERATIONS) { 
                ++numProcessedLines; 
                continue; 
            }
            if(resultsFileName == "tpch-q3-p2.csv" && numProcessedLines >= (2 * NUM_ITERATIONS)) {
                break;
            }
            if(resultsFileName == "tpch-q3-p3.csv" && numProcessedLines < (2 * NUM_ITERATIONS)) { 
                ++numProcessedLines; 
                continue; 
            }
            ++numProcessedLines; 

            size_t start;
            size_t end = 0;

            int inlineSnapshotIndex = 0;
            while ((start = line.find_first_not_of(',', end)) != std::string::npos)
            {
                end = line.find(',', start);
                if(inlineSnapshotIndex == 0) {
                    llvmParsingTimes.emplace_back(std::stod(line.substr(start, end - start)));
                } else if(inlineSnapshotIndex == 1) {
                    llvmIRLinkingTimes.emplace_back(std::stod(line.substr(start, end - start)));
                } else {
                    llvmIROptimizationTimes.emplace_back(std::stod(line.substr(start, end - start)));
                }
                ++inlineSnapshotIndex;
            }
        }
        runningSnapshotVectors.emplace_back(llvmParsingTimes);
        runningSnapshotVectors.emplace_back(llvmIRLinkingTimes);
        runningSnapshotVectors.emplace_back(llvmIROptimizationTimes);
        snapshotNames.emplace_back("\nLLVM IR Parsing       ");
        snapshotNames.emplace_back("LLVM IR Linking       ");
        snapshotNames.emplace_back("LLVM IR Optimization  ");
        // std::remove((RESULTS_PATH_BASE + "inlining/proxyFunctionsSize/inlining.csv").c_str());
        if(resultsFileName != "tpch-q3-p1.csv" && resultsFileName != "tpch-q3-p2.csv") {
            std::remove("llvmLambda.csv"); // Todo must be commented out for TPC-H-Q3
        }
    }

    std::ofstream fs("/home/alepping/results/" + resultsFileName, std::ios::out);
    if(fs.is_open()) {
        std::stringstream result;
        result << "                             ; Mean; Max; Min; Median; Q1; Q3; STD-DEV\n";
        std::stringstream rawDataResultStream;
        // std::string snapshotNamesAsString = "\n\nThe below raw data lines correspond to: \n";
        rawDataResultStream << "\n\nRaw Data: \n---------\n";
        
        for(size_t i = 0; i < runningSnapshotVectors.size(); ++i) {
            // Calculate Mean, Min, and Max of runtime measurements.
            auto currentSnapshotVector = runningSnapshotVectors.at(i);
            currentSnapshotVector.erase(currentSnapshotVector.begin()); // Remove first 'outlier' measurement
            // rawDataResultStream << snapshotNames.at(i) << "\n";
            // snapshotNamesAsString += "  " + snapshotNames.at(i) + '\n';
            for(auto rawValue : currentSnapshotVector) {
                rawDataResultStream << rawValue << ", ";
            }
            rawDataResultStream.seekp(-2, std::ios_base::end);
            rawDataResultStream<<"\0\0";
            auto snapshotMin = *std::min_element(currentSnapshotVector.begin(), currentSnapshotVector.end());
            auto snapshotMax = *std::max_element(currentSnapshotVector.begin(), currentSnapshotVector.end());
            auto snapshotMean = std::accumulate(currentSnapshotVector.begin(), currentSnapshotVector.end(), 0.0) / currentSnapshotVector.size();

            // Calculate std deviation of runtime measurements.
            std::vector<double> diff(currentSnapshotVector.size());
            std::transform(currentSnapshotVector.begin(), currentSnapshotVector.end(), diff.begin(), [snapshotMean](double x) { return x - snapshotMean; });
            double sq_sum = std::inner_product(diff.begin(), diff.end(), diff.begin(), 0.0);
            double snapshotStdDev = std::sqrt(sq_sum / currentSnapshotVector.size());

            // Calculate median & Q1, Q2
            size_t n = currentSnapshotVector.size() / 2;
            std::sort(currentSnapshotVector.begin(), currentSnapshotVector  .end());
            auto median = currentSnapshotVector.at(n);
            auto q1 = currentSnapshotVector.at(n/2);
            auto q2 = currentSnapshotVector.at(currentSnapshotVector.size() - 1 - n/2);
            

            // Create output string, log to console, write to file.
            std:: string currentSnapshotResult = snapshotNames.at(i) + "; " 
                + std::to_string(snapshotMean) + "; "
                + std::to_string(snapshotMax) + "; "
                + std::to_string(snapshotMin) + "; "
                + std::to_string(median) + "; "
                + std::to_string(q1) + "; "
                + std::to_string(q2) + "; "
                + std::to_string(snapshotStdDev) + '\n';
            NES_DEBUG(currentSnapshotResult);
            result << currentSnapshotResult;
            rawDataResultStream << "\n";
        }
        // snapshotNamesAsString.pop_back();
        // if(writeRawData) { result << snapshotNamesAsString << rawDataResultStream.str(); };
        if(writeRawData) { result << rawDataResultStream.str(); };
        fs.write(result.str().c_str(), result.str().size());
    }
}
}//namespace NES::ExecutionEngine::Experimental
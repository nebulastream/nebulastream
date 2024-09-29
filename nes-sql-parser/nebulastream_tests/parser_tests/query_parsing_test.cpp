//
// Created by Usama Bin Tariq on 25.09.24.
//

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include "gtest/gtest.h"
#include <nebula/parser/nebula_parser.hpp>
#include <unistd.h>

std::vector<std::string> read_file(const std::string &filename) {
    std::vector<std::string> lines;
    std::ifstream file(filename);

    if (!file.is_open()) {
        std::cerr << "Error: Could not open the file." << std::endl;
        return lines;
    }

    std::string line;
    while (std::getline(file, line)) {
        lines.push_back(line);
    }

    file.close();
    return lines;
}

TEST(PARSER_TEST, QUERY_PARSING_TEST) {
    //read queries from file
    const auto queries = read_file("./queries.sql");

    nebula::Parser parser;
    int total_parsed_queries = 0;

    for (const auto &query: queries) {
        parser.parse(query);

        ASSERT_EQ(++total_parsed_queries, parser.statements_collection->statements.size());
    }

    std::cout << "Total Parsed Queries: " << total_parsed_queries << std::endl;
}

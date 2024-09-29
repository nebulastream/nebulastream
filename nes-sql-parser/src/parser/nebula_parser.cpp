//
// Created by Usama Bin Tariq on 17.09.24.
//

#include "nebula/parser/nebula_parser.hpp"
#include <postgres_parser.hpp>
#include <nebula/parser/transformer/transformer.hpp>
#include <string>
#include <vector>
#include <memory>

namespace nebula {
    bool Parser::parse(const std::string &input) {
        pg_parser::PostgresParser parser;
        parser.Parse(input);


        if (!parser.parse_tree)
            return false;

        auto transformer = std::make_unique<Transformer>();
        return transformer->TransformParseTree(parser.parse_tree, statements_collection);
    }

    void Parser::PrintStatements() {
        statements_collection->Print();
    }

    Parser::Parser() {
    }
}

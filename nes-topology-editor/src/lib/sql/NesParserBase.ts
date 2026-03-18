import { Parser } from 'antlr4ng';

export abstract class NesParserBase extends Parser {
  SQL_standard_keyword_behavior = false;
  legacy_exponent_literal_as_decimal_enabled = false;
}

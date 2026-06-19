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

grammar AntlrSQL_new;

@lexer::postinclude {
#include <Util/DisableWarningsPragma.hpp>
DISABLE_WARNING_PUSH
DISABLE_WARNING(-Wlogical-op-parentheses)
DISABLE_WARNING(-Wunused-parameter)
}

@parser::postinclude {
#include <Util/DisableWarningsPragma.hpp>
DISABLE_WARNING_PUSH
DISABLE_WARNING(-Wlogical-op-parentheses)
DISABLE_WARNING(-Wunused-parameter)
}

@parser::members {
      bool SQL_standard_keyword_behavior = false;
      bool legacy_exponent_literal_as_decimal_enabled = false;
}

@lexer::members {
  bool isValidDecimal() {
    int nextChar = _input->LA(1);
    if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
      nextChar == '_') {
      return false;
    } else {
      return true;
    }
  }

  bool isHint() {
    int nextChar = _input->LA(1);
    if (nextChar == '+') {
      return true;
    } else {
      return false;
    }
  }
}

singleStatement: statement ';'? EOF;

terminatedStatement: statement ';';
multipleStatements: (statement (';' statement)* ';'?)? EOF;
statement: queryWithOptions | createStatement | dropStatement | showStatement | explainStatement;

explainStatement: EXPLAIN query;
EXPLAIN: 'EXPLAIN';
createStatement: CREATE createDefinition;
createDefinition: createLogicalSourceDefinition | createPhysicalSourceDefinition | createSinkDefinition | createWorkerDefinition;
createLogicalSourceDefinition: LOGICAL SOURCE sourceName=identifier schemaDefinition;

createPhysicalSourceDefinition: PHYSICAL SOURCE FOR logicalSource=identifier
                                TYPE type=identifier
                                optionsClause?;
FOR: 'FOR';
TYPE: 'TYPE';
optionsClause: (SET '(' options=namedConfigLiteralSeq? ')');
SET: 'SET';

namedConfigLiteralSeq: namedConfigLiteral (',' namedConfigLiteral)*;
namedConfigLiteral: literal AS qualifiedIdentifier;

createSinkDefinition: SINK sinkName=identifier schemaDefinition TYPE type=identifier optionsClause?;

createWorkerDefinition: WORKER hostaddr=STRING_LITERAL optionsClause?;

schemaDefinition: '(' columnDefinition (',' columnDefinition)* ')';
columnDefinition: identifier typeDefinition nullableDefinition?;

typeDefinition: DATA_TYPE;
nullableDefinition: NOT NULL;
NULL: 'NULL';

dropStatement: DROP dropSubject WHERE dropFilter;
DROP: 'DROP';
WHERE: 'WHERE';
dropSubject: dropQuery | dropSource | dropSink | dropWorker;
dropQuery: QUERY;
QUERY: 'QUERY';
dropSource: dropLogicalSourceSubject | dropPhysicalSourceSubject;
dropLogicalSourceSubject: LOGICAL SOURCE;
dropPhysicalSourceSubject: PHYSICAL SOURCE;
dropWorker: WORKER;
dropSink: SINK;

dropFilter: attr=identifier EQ value=literal;

showStatement: SHOW showSubject (WHERE showFilter)? (FORMAT identifier)?;
showSubject: showQueriesSubject | showLogicalSourcesSubject | showPhysicalSourcesSubject | showSinksSubject;
showQueriesSubject: QUERIES;
showLogicalSourcesSubject: LOGICAL SOURCES;
showPhysicalSourcesSubject: PHYSICAL SOURCES;
showSinksSubject: SINKS;

showFilter: attr=identifier EQ value=literal;

queryWithOptions: query optionsClause?;

query: projectionTerm fromTerm filterTerm? groupByTerm?;

projectionTerm: SELECT projectionSeq?;
SELECT: 'SELECT';
projectionSeq: projection (COMMA projection)*;
projection: ASTERISK | namedExpression;
ASTERISK: '*';
namedExpression: openScalarExpression (AS identifier)?;

filterTerm: WHERE openScalarExpression;
fromTerm: FROM openRelationalExpression;
FROM: 'FROM';
groupByTerm: GROUP_BY openScalarExpression (COMMA openScalarExpression)* UNARY_WINDOW_LITERAL;
GROUP_BY: GROUP BY;
fragment GROUP: 'GROUP ';
fragment BY: 'BY';


openRelationalExpression: closedRelationalExpression | unionSeq;
closedRelationalExpression: identifier | functionCallRelationalExpression | OPEN_PARANTHESIS openRelationalExpression CLOSE_PARANTHESIS | closedRelationalExpression AS identifier;
AS: 'AS';
joinRelationalExpression: closedRelationalExpression INNER_JOIN closedRelationalExpression ON openScalarExpression BINARY_WINDOW_LITERAL;

unionSeq: unionArg (UNION_ALL unionArg)*;
unionArg: query | joinRelationalExpression;


BINARY_WINDOW_LITERAL: WINDOW WINDOW_TYPE OPEN_PARANTHESIS IDENTIFIER COMMA IDENTIFIER COMMA INTERVAL_LITERAL CLOSE_PARANTHESIS;
UNARY_WINDOW_LITERAL: WINDOW WINDOW_TYPE OPEN_PARANTHESIS IDENTIFIER COMMA INTERVAL_LITERAL CLOSE_PARANTHESIS;
WINDOW_TYPE: TUMBLING | SLIDING;
fragment TUMBLING: 'TUMBLING';
fragment SLIDING: 'SLIDING';
fragment WINDOW: 'WINDOW';
ON: 'ON';
INNER_JOIN: INNER? JOIN;
fragment INNER: 'INNER';
fragment JOIN: 'JOIN';

UNION_ALL: UNION ALL?;
fragment ALL: 'ALL';
fragment UNION: 'UNION';


functionCallRelationalExpression: identifier OPEN_PARANTHESIS relFunctionCallArgument (COMMA relFunctionCallArgument)* CLOSE_PARANTHESIS;
relFunctionCallArgument: closedRelationalExpression | openScalarExpression;


openScalarExpression: infixScalarExpression | intervalExpression | closedScalarExpression;
closedScalarExpression: identifier | literal | prefixScalarExpression | castFunctionalScalarExpression | functionCallScalarExpression | OPEN_PARANTHESIS openScalarExpression CLOSE_PARANTHESIS;
prefixScalarExpression: prefixScalarOperator closedScalarExpression;
infixScalarExpression: closedScalarExpression infixScalarOperator closedScalarExpression;
functionCallScalarExpression: identifier OPEN_PARANTHESIS openScalarExpression (COMMA openScalarExpression)* CLOSE_PARANTHESIS;
castFunctionalScalarExpression: DATA_TYPE OPEN_PARANTHESIS openScalarExpression CLOSE_PARANTHESIS;
OPEN_PARANTHESIS: '(';
CLOSE_PARANTHESIS: ')';

COMMA: ',';


INTERVAL_LITERAL: INTERVAL LITERAL DATETIME_FIELD;
intervalExpression: INTERVAL openScalarExpression DATETIME_FIELD;
DATETIME_FIELD: YEAR | MONTH | DAY | HOUR | MINUTE | SECOND | MILLISECONDS;
fragment YEAR: 'YEAR';
fragment MONTH: 'MONTH';
fragment DAY: 'DAY';
fragment HOUR: 'HOUR';
fragment MINUTE: 'MINUTE';
fragment SECOND: 'SECOND';
fragment MILLISECONDS: 'MILLISECONDS';

INTERVAL: 'INTERVAL';

prefixScalarOperator: MINUS | NOT;
infixScalarOperator: MINUS | PLUS | EQ;



literal: LITERAL;
LITERAL: BOOLEAN_TYPE | STRING_LITERAL | SIGNED_INT_LITERAL | UNSIGNED_INT_LITERAL | FLOAT_LITERAL;

BOOL_LITERAL: TRUE | FALSE;
fragment TRUE: 'TRUE';
fragment FALSE: 'FALSE';


STRING_LITERAL: '\'' ( ~('\''|'\\') | ('\\' .) )* '\'' ;

SIGNED_INT_LITERAL: MINUS? DIGITS;
UNSIGNED_INT_LITERAL: DIGITS;

FLOAT_LITERAL: EXP_FRACTION | DECIMAL_FRACTION;
fragment EXP_FRACTION: (MINUS? 'e' DIGITS)? MINUS? DIGITS;
fragment DECIMAL_FRACTION : MINUS? DIGITS '.' DIGIT* | MINUS? '.' DIGITS;

MINUS: '-';
PLUS: '+';
NOT: 'NOT';
EQ: '=';




fragment DIGITS: DIGIT+;
fragment DIGIT : [0-9] ;

fragment LETTER : ('a'..'z'|'A'..'Z'|'_') ;

SINKS: 'SINKS';
SOURCES: 'SOURCES' | 'sources';
QUERIES: 'QUERIES' | 'queries';


DATA_TYPE: INTEGER_SIGNED_TYPE | INTEGER_UNSIGNED_TYPE | FLOATING_POINT_TYPE | CHAR_TYPE | VARSIZED_TYPE | BOOLEAN_TYPE;

fragment INTEGER_UNSIGNED_TYPE: UNSIGNED_TYPE_QUALIFIER INTEGER_BASES_TYPES | 'UINT8' | 'UINT16' | 'UINT32' | 'UINT64';
fragment INTEGER_SIGNED_TYPE: INTEGER_BASES_TYPES | 'INT64' | 'INT32' | 'INT16' | 'INT8';
fragment INTEGER_BASES_TYPES: TINY_INT_TYPE | SMALL_INT_TYPE | NORMAL_INT_TYPE | BIG_INT_TYPE;
fragment TINY_INT_TYPE: 'TINYINT';
fragment SMALL_INT_TYPE: 'SMALLINT';
fragment NORMAL_INT_TYPE: 'INT' | 'INTEGER';
fragment BIG_INT_TYPE: 'BIGINT';
fragment FLOATING_POINT_TYPE: 'FLOAT32' | 'FLOAT64';
fragment CHAR_TYPE: 'CHAR';
fragment VARSIZED_TYPE: 'VARSIZED';
fragment BOOLEAN_TYPE: 'BOOLEAN';

fragment UNSIGNED_TYPE_QUALIFIER: 'UNSIGNED ';



SHOW : 'SHOW';
FORMAT : 'FORMAT';
CREATE : 'CREATE';
SOURCE : 'SOURCE';
LOGICAL: 'LOGICAL';
PHYSICAL: 'PHYSICAL';
WORKER: 'WORKER';
SINK : 'SINK';

DOT: '.';

//Make sure that you add lexer rules for keywords before the identifier rule,
//otherwise it will take priority and your grammars will not work

WS: [ \r\n\t]+ -> channel(HIDDEN)
    ;

SIMPLE_COMMENT
    : '--' ('\\\n' | ~[\r\n])* '\r'? '\n'? -> channel(HIDDEN)
    ;

qualifiedIdentifier: IDENTIFIER (DOT IDENTIFIER)*;
identifier: IDENTIFIER;

IDENTIFIER: UNQUOTED_IDENTIFIER | QUOTED_IDENTIFIER;
QUOTED_IDENTIFIER: '"' ( ~'"' )* '"' ;

UNQUOTED_IDENTIFIER
    : LETTER (LETTER | DIGIT | '_')*
    ;

/// Catch-all for anything we can't recognize.
/// We use this to be able to ignore and recover all the text
/// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;

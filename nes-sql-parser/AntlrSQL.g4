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

grammar AntlrSQL;

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

explainStatement: EXPLAIN (FORMAT explainFormat)? query;
explainFormat: identifier | TEXT;

createStatement: CREATE createDefinition;
createDefinition: createLogicalSourceDefinition | createPhysicalSourceDefinition | createSinkDefinition | createWorkerDefinition | createModelDefinition;
createLogicalSourceDefinition: LOGICAL SOURCE sourceName=identifier schemaDefinition;

createPhysicalSourceDefinition: PHYSICAL SOURCE FOR logicalSource=identifier
                                TYPE type=identifier
                                optionsClause?;
optionsClause: (SET '(' options=namedConfigExpressionSeq ')');

createSinkDefinition: SINK sinkName=identifier schemaDefinition TYPE type=identifier optionsClause?;

createWorkerDefinition: WORKER hostaddr=STRING optionsClause?;

createModelDefinition: MODEL modelName=identifier '(' modelPath=STRING ')'
                       INPUT '(' modelInputField (',' modelInputField)* ')'
                       OUTPUT '(' modelOutputField (',' modelOutputField)* ')';
modelInputField: identifier typeDefinition;
modelOutputField: identifier typeDefinition;

schemaDefinition: '(' columnDefinition (',' columnDefinition)* ')';
columnDefinition: strictIdentifier typeDefinition nullableDefinition?;

typeDefinition: DATA_TYPE;
nullableDefinition: NOT NULLTOKEN;

dropStatement: DROP dropSubject WHERE dropFilter;
dropSubject: dropQuery | dropSource | dropSink | dropWorker | dropModel;
dropModel: MODEL;
dropQuery: QUERY;
dropSource: dropLogicalSourceSubject | dropPhysicalSourceSubject;
dropLogicalSourceSubject: LOGICAL SOURCE;
dropPhysicalSourceSubject: PHYSICAL SOURCE;
dropWorker: WORKER;
dropSink: SINK;

dropFilter: attr=strictIdentifier EQ value=constant;

showStatement: SHOW showSubject (WHERE showFilter)? (FORMAT showFormat)?;
showFormat: TEXT | JSON;
showSubject: QUERIES #showQueriesSubject
    | LOGICAL SOURCES #showLogicalSourcesSubject
    | PHYSICAL SOURCES (FOR logicalSourceName=strictIdentifier)? #showPhysicalSourcesSubject
    | SINKS #showSinksSubject
    | MODELS #showModelsSubject
    | VERSION #showVersionSubject;

showFilter: attr=strictIdentifier EQ value=constant;

queryWithOptions: query optionsClause?;

/// A query in Calcite's dialect, as written: its tokens up to the sink clause. Calcite parses and plans the query; only the
/// sink clause after INTO is ours. A query starts as SQL queries do, which tells it apart from the other statements.
query: queryText sinkClause;
queryText: (SELECT | WITH | VALUES | TABLE | '(') ~(INTO | SET | ';')*;

schema: SCHEMA schemaDefinition;

namedConfigExpression: (constant | schema) AS name=identifierChain;
namedConfigExpressionSeq: (namedConfigExpression (',' namedConfigExpression)*)?;

identifier: strictIdentifier;

strictIdentifier
    : IDENTIFIER #unquotedIdentifier
    | quotedIdentifier #quotedIdentifierAlternative;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

BACKQUOTED_IDENTIFIER
    : '"' ( ~'"' )* '"'
    ;

identifierChain: strictIdentifier ('.' strictIdentifier)*;

sinkClause: INTO sink (',' sink)*;

sink: identifier | anonymousSink;

anonymousSink
    : type=identifier '(' parameters=namedConfigExpressionSeq ')'
    ;

number
    : MINUS? INTEGER_VALUE              #integerLiteral
    | MINUS? FLOAT_LITERAL              #floatLiteral
    ;

unsignedIntegerLiteral: INTEGER_VALUE;

signedIntegerLiteral: MINUS INTEGER_VALUE;

constant
    : NULLTOKEN                                                                                #nullLiteral
    | identifier STRING                                                                        #typeConstructor
    | number                                                                                   #numericLiteral
    | booleanValue                                                                             #booleanLiteral
    | STRING                                                                                  #stringLiteral
    ;

booleanValue
    : TRUE | FALSE | BOOLEAN_VALUE
    ;

///****************************
/// Start of the keywords list
///****************************
ALL: 'ALL' | 'all';
AND: 'AND' | 'and';
ANY: 'ANY';
AS: 'AS' | 'as';
ASC: 'ASC' | 'asc';
AT: 'AT';
BETWEEN: 'BETWEEN' | 'between';
BY: 'BY' | 'by';
CAST: 'CASTTOTYPE' | 'casttotype';
COMMENT: 'COMMENT';
CUBE: 'CUBE';
DELETE: 'DELETE';
DESC: 'DESC' | 'desc';
DISTINCT: 'DISTINCT';
DIV: 'DIV';
DROP: 'DROP';
ELSE: 'ELSE';
END: 'END';
ESCAPE: 'ESCAPE';
EXISTS: 'EXISTS';
FALSE: 'FALSE';
FIRST: 'FIRST';
FOR: 'FOR';
FROM: 'FROM' | 'from';
FULL: 'FULL' | 'full';
GROUP: 'GROUP' | 'group';
GROUPING: 'GROUPING';
HAVING: 'HAVING' | 'having';
IF: 'IF';
IN: 'IN' | 'in';
INNER: 'INNER' | 'inner';
INSERT: 'INSERT' | 'insert';
INTO: 'INTO' | 'into';
IS: 'IS' | 'is';
JOIN: 'JOIN' | 'join';
LAST: 'LAST';
LEFT: 'LEFT' | 'left';
LIKE: 'LIKE';
LIMIT: 'LIMIT' | 'limit';
LIST: 'LIST';
MERGE: 'MERGE' | 'merge';
NANTOKEN: 'NAN' | 'nan' | 'NaN';
NATURAL: 'NATURAL';
NOT: 'NOT' | 'not' | '!';
NULLTOKEN:'NULL';
NULLS: 'NULLS';
OF: 'OF';
ON: 'ON' | 'on';
OR: 'OR' | 'or';
ORDER: 'ORDER' | 'order';
OUTER: 'OUTER' | 'outer';
QUERY: 'QUERY';
RECOVER: 'RECOVER';
RIGHT: 'RIGHT' | 'right';
RLIKE: 'RLIKE' | 'REGEXP';
ROLLUP: 'ROLLUP';
SCHEMA: 'SCHEMA';
SELECT: 'SELECT' | 'select';
SETS: 'SETS';
SOME: 'SOME';
START: 'START';
TABLE: 'TABLE' | 'table';
TO: 'TO';
TRUE: 'TRUE';
TYPE: 'TYPE';
UNION: 'UNION' | 'union';
UNKNOWN: 'UNKNOWN';
USE: 'USE';
USING: 'USING';
VALUES: 'VALUES' | 'values';
WHEN: 'WHEN';
WHERE: 'WHERE' | 'where';
WINDOW: 'WINDOW' | 'window';
WITH: 'WITH' | 'with';
SET: 'SET';
TUMBLING: 'TUMBLING' | 'tumbling';
SLIDING: 'SLIDING' | 'sliding';
THRESHOLD : 'THRESHOLD'|'threshold';
SIZE: 'SIZE' | 'size';
ADVANCE: 'ADVANCE' | 'advance';
MS: 'MS' | 'ms';
SEC: 'SEC' | 'sec';
MINUTE: 'MINUTE' | 'minute' | 'MINUTES' | 'minutes';
HOUR: 'HOUR' | 'hour' | 'HOURS' | 'hours';
DAY: 'DAY' | 'day' | 'DAYS' | 'days';
MIN: 'MIN' | 'min';
MAX: 'MAX' | 'max';
AVG: 'AVG' | 'avg';
SUM: 'SUM' | 'sum';
COUNT: 'COUNT' | 'count';
MEDIAN: 'MEDIAN' | 'median';
WATERMARK: 'WATERMARK' | 'watermark';
OFFSET: 'OFFSET' | 'offset';
CSV_FORMAT : 'CSV_FORMAT';
AT_MOST_ONCE : 'AT_MOST_ONCE';
AT_LEAST_ONCE : 'AT_LEAST_ONCE';
JSON: 'JSON';
TEXT: 'TEXT';
EXPLAIN: 'EXPLAIN' | 'explain';
MODEL: 'MODEL';
MODELS: 'MODELS';
MODEL_INFERENCE: 'MODEL_INFERENCE';
INPUT: 'INPUT';
OUTPUT: 'OUTPUT';

///--NebulaSQL-KEYWORD-LIST-END
///****************************
/// End of the keywords list
///****************************



BOOLEAN_VALUE: 'true' | 'false';
EQ  : '=' | '==';
NSEQ: '<=>';
NEQ : '<>';
NEQJ: '!=';
LT  : '<';
LTE : '<=' | '!>';
GT  : '>';
GTE : '>=' | '!<';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
TILDE: '~';
AMPERSAND: '&';
PIPE: '|';
CONCAT_PIPE: '||';
HAT: '^';

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    ;

INTEGER_VALUE
    : DIGIT+
    ;

FLOAT_LITERAL
    : DIGIT+ EXPONENT?
    | DECIMAL_DIGITS EXPONENT? {isValidDecimal()}?
    ;


fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : ('a'..'z'|'A'..'Z'|'_')
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;


SINKS: 'SINKS';
SOURCES: 'SOURCES' | 'sources';
QUERIES: 'QUERIES' | 'queries';


DATA_TYPE: INTEGER_SIGNED_TYPE | INTEGER_UNSIGNED_TYPE | FLOATING_POINT_TYPE | CHAR_TYPE | VARSIZED_TYPE | BOOLEAN_TYPE;

INTEGER_UNSIGNED_TYPE: UNSIGNED_TYPE_QUALIFIER INTEGER_BASES_TYPES | 'UINT8' | 'UINT16' | 'UINT32' | 'UINT64';
INTEGER_SIGNED_TYPE: INTEGER_BASES_TYPES | 'INT64' | 'INT32' | 'INT16' | 'INT8';
INTEGER_BASES_TYPES: TINY_INT_TYPE | SMALL_INT_TYPE | NORMAL_INT_TYPE | BIG_INT_TYPE;
TINY_INT_TYPE: 'TINYINT';
SMALL_INT_TYPE: 'SMALLINT';
NORMAL_INT_TYPE: 'INT' | 'INTEGER';
BIG_INT_TYPE: 'BIGINT';
FLOATING_POINT_TYPE: 'FLOAT32' | 'FLOAT64';
CHAR_TYPE: 'CHAR';
VARSIZED_TYPE: 'VARSIZED';
BOOLEAN_TYPE: 'BOOLEAN';

UNSIGNED_TYPE_QUALIFIER: 'UNSIGNED ';



SHOW : 'SHOW';
FORMAT : 'FORMAT' | 'format';
CREATE : 'CREATE';
SOURCE : 'SOURCE';
LOGICAL: 'LOGICAL' | 'logical';
PHYSICAL: 'PHYSICAL';
WORKER: 'WORKER';
SINK : 'SINK';
VERSION : 'VERSION' | 'version';

//Make sure that you add lexer rules for keywords before the identifier rule,
//otherwise it will take priority and your grammars will not work

SIMPLE_COMMENT
    : '--' ('\\\n' | ~[\r\n])* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' {!isHint()}? (BRACKETED_COMMENT|.)*? '*/' -> channel(HIDDEN)
    ;

IDENTIFIER
    : LETTER (LETTER | DIGIT | '_')*
    ;

/// Catch-all for anything we can't recognize.
/// We use this to be able to ignore and recover all the text
/// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;

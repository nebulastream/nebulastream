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
statement: query | createStatement | dropStatement | showStatement;

createStatement: CREATE createDefinition;
createDefinition: createLogicalSourceDefinition | createPhysicalSourceDefinition | createSinkDefinition;
createLogicalSourceDefinition: LOGICAL SOURCE sourceName=identifier schemaDefinition fromQuery?;

createPhysicalSourceDefinition: PHYSICAL SOURCE FOR logicalSource=identifier
                                TYPE type=identifier
                                (SET '(' options=namedConfigExpressionSeq ')')?;

createSinkDefinition: SINK sinkName=identifier schemaDefinition TYPE type=identifier (SET '(' options=namedConfigExpressionSeq ')')?;


schemaDefinition: '(' columnDefinition (',' columnDefinition)* ')';
columnDefinition: identifierChain typeDefinition;

typeDefinition: DATA_TYPE;

fromQuery: AS query;

dropStatement: DROP dropSubject;
dropSubject: dropQuery | dropSource | dropSink;
dropQuery: QUERY id=unsignedIntegerLiteral;
dropSource: dropLogicalSourceSubject | dropPhysicalSourceSubject;
dropLogicalSourceSubject: LOGICAL SOURCE name=strictIdentifier;
dropPhysicalSourceSubject: PHYSICAL SOURCE id=unsignedIntegerLiteral;
dropSink: SINK name=strictIdentifier;

showStatement: SHOW showSubject (WHERE showFilter)? (FORMAT showFormat)?;
showFormat: TEXT | JSON;
showSubject: QUERIES #showQueriesSubject
    | LOGICAL SOURCES #showLogicalSourcesSubject
    | PHYSICAL SOURCES (FOR logicalSourceName=strictIdentifier)? #showPhysicalSourcesSubject
    | SINKS #showSinksSubject;

showFilter: attr=strictIdentifier EQ value=constant;

query : queryTerm queryOrganization;

queryOrganization:
         (ORDER BY order+=sortItem (',' order+=sortItem)*)?
         (LIMIT (ALL | limit=INTEGER_VALUE))?
         (OFFSET offset=INTEGER_VALUE)?
         ;

queryTerm: queryPrimary #primaryQuery
         |  left=queryTerm setoperator=UNION right=queryTerm  #setOperation
         ;

queryPrimary
    : querySpecification                                                    #queryPrimaryDefault
    | fromStatement                                                         #fromStmt
    | TABLE multipartIdentifier                                             #table
    | inlineTable                                                           #inlineTableDefault1
    | '(' query ')'                                                         #subquery
    ;
/// new layout to be closer to traditional SQL
querySpecification: selectClause fromClause whereClause? windowedAggregationClause? havingClause? sinkClause?;


fromClause: FROM relation (',' relation)*;

relation
    : relationPrimary joinRelation*
    ;

joinRelation
    : (joinType) JOIN right=relationPrimary joinCriteria? windowClause
    | NATURAL joinType JOIN right=relationPrimary windowClause
    ;

joinType
    : INNER?
    ;

joinCriteria
    : ON booleanExpression
    ;

relationPrimary
    : multipartIdentifier tableAlias          #tableName
    | '(' query ')'  tableAlias               #aliasedQuery
    | '(' relation ')' tableAlias             #aliasedRelation
    | inlineTable                             #inlineTableDefault2
    | inlineSource                            #inlineDefinedSource
    ;

inlineSource
    : type=identifier '(' parameters=namedConfigExpressionSeq ')'
    ;

schema: SCHEMA schemaDefinition
 ;

fromStatement: fromClause fromStatementBody+;

fromStatementBody: selectClause whereClause? groupByClause?;

selectClause : SELECT (hints+=hint)* namedExpressionSeq;

whereClause: WHERE booleanExpression;

havingClause: HAVING booleanExpression;

inlineTable
    : VALUES expression (',' expression)* tableAlias
    ;

tableAlias
    : (AS? identifier identifierList?)?
    ;

multipartIdentifier
    : parts+=errorCapturingIdentifier ('.' parts+=errorCapturingIdentifier)*
    ;

namedConfigExpression: (constant | schema) AS name=identifierChain;

namedExpression
    : expression AS name=identifier
    | expression
    ;

identifier: strictIdentifier;

strictIdentifier
    : IDENTIFIER #unquotedIdentifier
    | quotedIdentifier #quotedIdentifierAlternative;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

identifierChain: strictIdentifier ('.' strictIdentifier)*;

identifierList
    : '(' identifierSeq ')'
    ;

identifierSeq
    : ident+=errorCapturingIdentifier (',' ident+=errorCapturingIdentifier)*
    ;

errorCapturingIdentifier
    : identifier errorCapturingIdentifierExtra
    ;

errorCapturingIdentifierExtra
    : (MINUS identifier)+    #errorIdent
    |                        #realIdent
    ;

namedConfigExpressionSeq: (namedConfigExpression (',' namedConfigExpression)*)?;
namedExpressionSeq
    : namedExpression (',' namedExpression)*
    ;

expression
    : valueExpression
    | booleanExpression
    | identifier
    | schema
    ;

booleanExpression
    : NOT booleanExpression                                        #logicalNot
    | EXISTS '(' query ')'                                         #exists
    | valueExpression predicate?                                   #predicated
    | left=booleanExpression op=AND right=booleanExpression  #logicalBinary
    | left=booleanExpression op=OR right=booleanExpression   #logicalBinary
    ;

/// Problem fixed that the querySpecification rule could match an empty string
windowedAggregationClause:
    groupByClause? windowClause watermarkClause?
    | windowClause groupByClause? watermarkClause?;

groupByClause
    : GROUP BY groupingExpressions+=expression (',' groupingExpressions+=expression)* (
      WITH kind=ROLLUP
    | WITH kind=CUBE
    | kind=GROUPING SETS '(' groupingSet (',' groupingSet)* ')')?
    | GROUP BY kind=GROUPING SETS '(' groupingSet (',' groupingSet)* ')'
    ;

groupingSet
    : '(' (expression (',' expression)*)? ')'
    | expression
    ;

windowClause
    : WINDOW windowSpec
    ;

watermarkClause: WATERMARK '(' watermarkParameters ')';

watermarkParameters: watermarkIdentifier=identifier ',' watermark=INTEGER_VALUE watermarkTimeUnit=timeUnit;
/// Adding Threshold Windows
windowSpec:
    timeWindow #timeBasedWindow
    | countWindow #countBasedWindow
    | conditionWindow #thresholdBasedWindow
    ;

timeWindow
    : TUMBLING '(' (timestampParameter ',')?  sizeParameter ')'                       #tumblingWindow
    | SLIDING '(' (timestampParameter ',')? sizeParameter ',' advancebyParameter ')' #slidingWindow
    ;

countWindow:
    TUMBLING '(' INTEGER_VALUE ')'    #countBasedTumbling
    ;

conditionWindow
    : THRESHOLD '(' conditionParameter (',' thresholdMinSizeParameter)? ')' #thresholdWindow
    ;

conditionParameter: expression;
thresholdMinSizeParameter: INTEGER_VALUE;

sizeParameter: SIZE INTEGER_VALUE timeUnit;

advancebyParameter: ADVANCE BY INTEGER_VALUE timeUnit;

timeUnit: MS
        | SEC
        | MINUTE
        | HOUR
        | DAY
        ;

timestampParameter: name=identifier;

functionName:  IDENTIFIER | AVG | MAX | MIN | SUM | COUNT | MEDIAN;

sinkClause: INTO sink (',' sink)*;

sink: identifier | inlineSink;

inlineSink
    : type=identifier '(' parameters=namedConfigExpressionSeq ')'
    ;

nullNotnull
    : NOT? NULLTOKEN
    ;

streamName: IDENTIFIER;

fileFormat: CSV_FORMAT;

sortItem
    : expression ordering=(ASC | DESC)? (NULLS nullOrder=(LAST | FIRST))?
    ;

predicate
    : NOT? kind=BETWEEN lower=valueExpression AND upper=valueExpression
    | NOT? kind=IN '(' expression (',' expression)* ')'
    | NOT? kind=IN '(' query ')'
    | NOT? kind=RLIKE pattern=valueExpression
    | NOT? kind=LIKE quantifier=(ANY | SOME | ALL) ('('')' | '(' expression (',' expression)* ')')
    | NOT? kind=LIKE pattern=valueExpression (ESCAPE escapeChar=STRING)?
    | IS nullNotnull
    | IS NOT? kind=(TRUE | FALSE | UNKNOWN)
    | IS NOT? kind=DISTINCT FROM right=valueExpression
    ;


valueExpression
    : (functionName | typeDefinition) '(' (argument+=expression (',' argument+=expression)*)? ')'                 #functionCall
    | op=(MINUS | PLUS | TILDE) valueExpression                                        #arithmeticUnary
    | left=valueExpression op=(ASTERISK | SLASH | PERCENT | DIV) right=valueExpression #arithmeticBinary
    | left=valueExpression op=(PLUS | MINUS | CONCAT_PIPE) right=valueExpression       #arithmeticBinary
    | left=valueExpression op=AMPERSAND right=valueExpression                          #arithmeticBinary
    | left=valueExpression op=HAT right=valueExpression                                #arithmeticBinary
    | left=valueExpression op=PIPE right=valueExpression                               #arithmeticBinary
    | left=valueExpression comparisonOperator right=valueExpression                          #comparison
    | primaryExpression                                                                      #valueExpressionDefault
    ;

comparisonOperator
    : EQ | NEQ | NEQJ | LT | LTE | GT | GTE | NSEQ
    ;

hint
    : '/*+' hintStatements+=hintStatement (','? hintStatements+=hintStatement)* '*/'
    ;

hintStatement
    : hintName=identifier
    | hintName=identifier '(' parameters+=primaryExpression (',' parameters+=primaryExpression)* ')'
    ;

primaryExpression
    : ASTERISK                                                                                 #star
    | qualifiedName '.' ASTERISK                                                               #star
    | base=primaryExpression '.' fieldName=identifier                                          #dereference
    | '(' query ')'                                                                            #subqueryExpression
    | '(' namedExpression (',' namedExpression)+ ')'                                           #rowConstructor
    | '(' expression ')'                                                                       #parenthesizedExpression
    | constant                                                                                 #constantDefault
    | identifier                                                                               #columnReference
    ;

qualifiedName
    : identifier ('.' identifier)*
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
    : TRUE | FALSE
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
FULL: 'FULL';
GROUP: 'GROUP' | 'group';
GROUPING: 'GROUPING';
HAVING: 'HAVING' | 'having';
IF: 'IF';
IN: 'IN' | 'in';
INNER: 'INNER' | 'inner';
INSERT: 'INSERT' | 'insert';
INTO: 'INTO' | 'into';
IS: 'IS'  'is';
JOIN: 'JOIN' | 'join';
LAST: 'LAST';
LEFT: 'LEFT';
LIKE: 'LIKE';
LIMIT: 'LIMIT' | 'limit';
LIST: 'LIST';
MERGE: 'MERGE' | 'merge';
NATURAL: 'NATURAL';
NOT: 'NOT' | 'not' | '!';
NULLTOKEN:'NULL';
NULLS: 'NULLS';
OF: 'OF';
ON: 'ON' | 'on';
OR: 'OR' | 'or';
ORDER: 'ORDER' | 'order';
QUERY: 'QUERY';
RECOVER: 'RECOVER';
RIGHT: 'RIGHT';
RLIKE: 'RLIKE' | 'REGEXP';
ROLLUP: 'ROLLUP';
SCHEMA: 'SCHEMA';
SELECT: 'SELECT' | 'select';
SETS: 'SETS';
SOME: 'SOME';
START: 'START';
TABLE: 'TABLE';
TO: 'TO';
TRUE: 'TRUE';
TYPE: 'TYPE';
UNION: 'UNION' | 'union';
UNKNOWN: 'UNKNOWN';
USE: 'USE';
USING: 'USING';
VALUES: 'VALUES';
WHEN: 'WHEN';
WHERE: 'WHERE' | 'where';
WINDOW: 'WINDOW' | 'window';
WITH: 'WITH';
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
LOCALHOST: 'LOCALHOST' | 'localhost';
CSV_FORMAT : 'CSV_FORMAT';
AT_MOST_ONCE : 'AT_MOST_ONCE';
AT_LEAST_ONCE : 'AT_LEAST_ONCE';
JSON: 'JSON';
TEXT: 'TEXT';
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
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
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
FORMAT : 'FORMAT';
CREATE : 'CREATE';
SOURCE : 'SOURCE';
LOGICAL: 'LOGICAL';
PHYSICAL: 'PHYSICAL';
SINK : 'SINK';

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

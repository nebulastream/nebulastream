
grammar NesCEP;

query
	: cepPattern+ EOF
	;

cepPattern
	:
	PATTERN NAME SEP compositeEventExpressions
	FROM inputStreams
	(WHERE whereExp)?
	(WITHIN windowConstraints)?
	(CONSUMING option)?
	(SELECT outputExpression)?
	INTO sinkList
;

inputStreams
	: inputStream (COMMA inputStream)*
	;

inputStream
  : NAME (AS NAME)?
  ;

compositeEventExpressions
	: LPARENTHESIS? listEvents RPARENTHESIS?
	;

whereExp
	: expression
	;

windowConstraints
	: windowType LBRACKET timeConstraints RBRACKET
	;

windowType
    : 'SLIDING' | 'INTERVAL' | 'TUMBLING'
    ;

timeConstraints
   : NAME COMMA constraint interval (COMMA constraint interval)*
   ;

constraint
  : 'SIZE'
  | 'SLIDE'
  | 'INTERVAL'
   ;

interval
	:
	INT (WS)? intervalType
	;

INT:
  DEC_DIGIT+
  ;

intervalType
    : 'QUARTER' | 'MONTH' | 'DAY' | 'HOUR'
    | 'MINUTE' | 'WEEK' | 'SECOND' | 'MICROSECOND'
    ;

option
	: ALL
	| NONE
	;

outputExpression
	: NAME SEP LBRACKET outAttribute (COMMA outAttribute)* RBRACKET
	;

outAttribute
    :
    NAME EQUAL attVal
    ;

sinkList
	: sink (COMMA sink)*
	;

listEvents
   : eventElem (operatorRule eventElem)*
   ;

eventElem
   :  NOT? event
   |  NOT? LPARENTHESIS listEvents RPARENTHESIS
   ;

event
   : NAME quantifiers?
   ;

quantifiers
   : STAR
   | PLUS
   | LBRACKET (consecutiveOption)? INT (PLUS)? RBRACKET
   | LBRACKET (consecutiveOption)? iterMin D_POINTS iterMax RBRACKET
   ;

iterMax:
 INT
;

iterMin:
    INT
;

consecutiveOption: (ANY)? NEXT;

parameterName:
    NAME
    ;

operatorRule
   : AND
   | OR
   | sequence
   ;

sequence
	: SEQ
	| contiguity
	;

contiguity
	: NEXT
	| ANY NEXT
	;

  sinkType
  : 'KAFKA'
  | 'FILE'
  | 'MQTT'
  | 'NETWORK'
  | 'NULLOUTPUT'
  | 'OPC'
  | 'PRINT'
  | 'ZMQ'
  ;

nullNotnull
    : NOT? NULLTOKEN
    ;

constant
    : QUOTE NAME QUOTE | FLOAT | INT | NAME
    ;

FLOAT:
  DEC_DIGIT+ '.' DEC_DIGIT+
  ;

expressions
    : expression (COMMA expression)*
    ;

// Simplified approach for expression
expression
    : NOT_OP expression                                             #notExpression
    | expression logicalOperator expression                         #logicalExpression
    | predicate IS NOT? testValue=(TRUE | FALSE | UNKNOWN)          #isExpression
    | predicate                                                     #predicateExpression
    ;

predicate
    : predicate NOT? IN LPARENTHESIS expressions RPARENTHESIS    	#inPredicate
    | predicate IS nullNotnull                                      #isNullPredicate
    | left=predicate comparisonOperator right=predicate             #binaryComparisonPredicate
    | expressionAtom                         			            #expressionAtomPredicate
    ;

expressionAtom
    : eventAttribute                                                #attributeAtom
    | unaryOperator WS? expressionAtom                              #unaryExpressionAtom
    | BINARY expressionAtom                                         #binaryExpressionAtom
    | LPARENTHESIS expression (COMMA expression)* RPARENTHESIS      #nestedExpressionAtom
    | left=expressionAtom bitOperator right=expressionAtom          #bitExpressionAtom
    | left=expressionAtom mathOperator right=expressionAtom         #mathExpressionAtom
    | constant                                                      #constantExpressionAtom
    ;

eventAttribute
    : aggregation LPARENTHESIS expressions RPARENTHESIS
    | eventIteration (POINT attribute)?
    | NAME POINT attribute
    ;

eventIteration:
   NAME LBRACKET (mathExpression)? RBRACKET
;

mathExpression:
  left=expressionAtom mathOperator right=expressionAtom
  | constant (D_POINTS constant)?
;

aggregation:
 | AVG
 | SUM
 | MIN
 | MAX
 | COUNT
 ;

PROTOCOL
    : 'http'
    | 'https'
    | 'ws'
    | 'wss'
    ;

FILETYPE
    : 'csv'
    | 'txt'
    ;

value:
    attribute POINT FILETYPE
    | attribute
    | URL;

attribute
    : NAME
    ;

PORT
    : INT                        // Numeric port
    ;

attVal
    : IF condition
    | eventAttribute
    | event
    | expression
    | boolRule
    ;

boolRule: TRUE | FALSE;

condition: LPARENTHESIS expression COMMA attVal COMMA attVal RPARENTHESIS;

unaryOperator
    : '+' | '-' | NOT
    ;

comparisonOperator
    : '=' WS? '=' | '>' | '<' | '<' WS? '=' | '>' WS? '='
    | '<' WS? '>' | '!' WS? '=' | '<' WS? '=' WS? '>' |'='
    ;

logicalOperator
    : LOGAND | LOGXOR | LOGOR
    ;

bitOperator
    : '<' '<' | '>' '>' | '&' | '^' | '|'
    ;

mathOperator
    : '*' | '/' | '%'  | '+' | '-' | '--'
    ;

//White space skipping
WS
   : [ \r\n\t]+ -> skip
   ;

sink
  : sinkType LPARENTHESIS parameters RPARENTHESIS #sinkWithParameters
  | sinkType                                      #sinkWithoutParameters
  ;

parameters
   : parameter value (COMMA parameter value)*
   ;

parameter
  : 'fileName'
  | 'topic'
  | 'address'
   ;

//KEYWORDs
FROM:                              'FROM';
PATTERN:                           'PATTERN';
WHERE:                             'WHERE';
WITHIN:                            'WITHIN';
CONSUMING:						   'CONSUMING';
SELECT:							   'SELECT';
INTO:							   'INTO';
ALL:							   'ALL';
ANY:							   'ANY';
SEP:							   ':=' ;
COMMA:							   ','  ;
LPARENTHESIS: 					   '('  ;
RPARENTHESIS: 					   ')'  ;
NOT:							   'NOT'  ;
NOT_OP:                             '!';
SEQ:							   'SEQ'  ;
NEXT:							   'NEXT';
AND:							   'AND'  ;
OR:								   'OR'  ;
STAR:							   '*'  ;
PLUS:							   '+'  ;
D_POINTS:						   ':'	;
LBRACKET:						   '['  ;
RBRACKET:						   ']'  ;
XOR:							   'XOR';
IN:                                'IN';
IS:                                'IS';
NULLTOKEN:                         'NULL';
BETWEEN:						   'BETWEEN';
BINARY:							   'BINARY';
TRUE:							   'TRUE';
FALSE:							   'FALSE';
UNKNOWN:                           'UNKNOWN';
QUARTER:                           'QUARTER';
MONTH:                             'MONTH';
DAY:                               'DAY';
HOUR:                              'HOUR';
MINUTE:                            'MINUTE';
WEEK:                              'WEEK';
SECOND:                            'SECOND';
MICROSECOND:                       'MICROSECOND';
AS:                                'AS';
EQUAL:                             '=';
KAFKA:                             'Kafka';
FILE:                              'File';
MQTT:                              'MQTT';
NETWORK:                           'Network';
NULLOUTPUT:                        'NullOutput';
OPC:                               'OPC';
PRINT:                             'Print';
ZMQ:                               'ZMQ';
fileName:                           'fileName';
topic:                              'topic';
address:                            'address';
POINT:                              '.';
QUOTE:                               '"';
AVG:                               'AVG';
SUM:                               'SUM';
MIN:                               'MIN';
MAX:                               'MAX';
COUNT:                             'COUNT';
IF:                                'IF';
LOGOR:                             '||';
LOGAND:                            '&&';
LOGXOR:                            '^';
NONE:                              'NONE';

URL
    : PROTOCOL '://' NAME (':' PORT)? (PATH)? // PATH is optional here
    ;

NAME
    : (ALPHA | DEC_DIGIT)+ ( ('_' |'-'|'/')+ (ID | INT | NAME)*)?
	;

ID
	:
	(ALPHA | INT)+
	;

PATH
    : ('/')+               // Optional path, disallowing spaces
    ;

fragment DEC_DIGIT:                  [0-9];

fragment ALPHA
    : [a-zA-Z]+                      // Alphabetic characters
    ;

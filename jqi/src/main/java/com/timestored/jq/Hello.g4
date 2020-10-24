grammar Hello;

r:   stat+ EOF;

stat:   (expr | expr ';'| expr NEWLINE)+           #printExpr
    |   NEWLINE                     # blank
    ;

expr:   <assoc=right> datatype                                      # datatypk
    |   <assoc=right> left=expr operator=BINOP right=expr   # BinDo
    |   <assoc=right> operator=MONOP expr                   # MonDo
    |   <assoc=right> operator=MONOP '[' expr ']'                   # MonDo
    |   <assoc=right> ID                                            # myid
    |   <assoc=right> '(' expr ')'                                  # parens
    ;

// LONG/FLOAT are the default types the other atoms/list use postfixes to set type
datatype: (SYMBOL SYMBOL+) #symbolList
         | BOOLLIST #boolList | BOOL #bool
         | LONGLIST 'h' #shortList | LONGLIST 'i' #intList | LONGLIST 'j' #longList | LONGLIST #longList
         | FLOATLIST 'e' #floatList |FLOATLIST 'f' #doubleList |FLOATLIST #doubleList
         | FLOAT 'e' #float | FLOAT 'f' #double | FLOAT #double
         | LONG  'h' #short | LONG  'i' #int | LONG  'j' #long | LONG #long
         | CHARLIST #charList
         | '(' ')' #emptyList
         | CHAR #char | SYMBOL #symbol;


WS  :   (' '|'\r'|'\n'|'\t')+ -> skip;
NEWLINE:[\r\n]+;
COMMENT:'/*' .*? '*/' -> channel(HIDDEN);
LINE_COMMENT:'//' ~[\r\n]* -> channel(HIDDEN);

LONGLIST:LONG (' ' LONG)+;
FLOAT:([-])* Floaty;

FLOATLIST:FLOAT (' ' (LONG | FLOAT))+
      | (LONG | LONGLIST)+ (' ' FLOAT) (' ' (LONG | FLOAT))*;

LONG :   ([-])* [0-9]+ | '0N' | '0W' | '-0W' ;
BOOL:[01] 'b';
BOOLLIST: [10]+ 'b';


MONOP:('til'|'desc'|'asc'|'++'|'--'|'til'|'get'|'abs'|'all'|'any'|'avg'|'avgs'|'exp'|'floor'|'ceiling'
    |'cos'|'sin'|'tan'|'acos'|'asin'|'atan'|'exp'|'log'|'except'|'fills'|'flip'
    |'first'|'last'|'distinct'|'count'|'type'|'attr'|'reciprocal'|'sqrt'
    |'svar'|'sdev'|'var'|'dev'|'differ'|'getenv'|'setenv'|'group'|'iasc'
    |'idesc'|'key');
BINOP:(':'|'<'|'>'|'<='|'>='|'&'|'|'|'~'|'?'|'='|'<>'|'+:'|'!'|'*:'|'^'|'+'|'-'|'*'|'%'|'@'|'and'|'or'|'set'|'mod'
    |'$'|'_'|'div'|'each'|'peach'|'except'|'inter'|'union'|'deltas'|'cut'|'cross'|'bin');
ID:	Letter LetterOrDigit*;

SYMBOL: '`' (Letter|Digit|'_'|'.'|'/'|':')*;

CHARLIST: ('"' '"') | ('"' (ESC|.) (ESC|.)+? '"');
CHAR: '"' (ESC|.) '"';
fragment ESC:'\\"' | '\\\\';

Digits:Digit (Digit)*;
Digit: '0' | [1-9];

fragment Floaty:	Digits '.' Digits? ExponentPart?
	|	'.' Digits ExponentPart?
	|	Digits ExponentPart
	'0n' | '0w' | '-0w' | '0N' | '0W' | '-0W';

fragment ExponentPart: [eE] [+-]? Digits;
fragment Letter:[a-zA-Z_];
fragment LetterOrDigit:	[a-zA-Z0-9_];



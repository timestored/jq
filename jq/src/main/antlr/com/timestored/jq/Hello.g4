grammar Hello;

@header {
package com.timestored.jq;
}

r:   stat+ EOF;

stat:   (';' expr| NEWLINE expr)           #printExpr
    |   (WS* NEWLINE|WS* ';')                     # blank
    ;

expr:   <assoc=right> datatype                              # datatypk
    |   <assoc=right> expr expr+                            # apply
    |   <assoc=right> left=expr operator=BINOP right=expr   # BinDo
    |   <assoc=right> operator=BINOP ('[' left=expr? ';' right=expr? ']')? # Bin
    |   <assoc=right> operator=(MONOP|'where') ('[' expr ']')?        # Mon
    |   <assoc=right> '(' expr ')'                          # parens
    |   <assoc=right> '(' expr (';' expr)+ ')'              # nestedList
    |   <assoc=right> table                                 # tbl
    |   <assoc=right> function                              # func
    |   <assoc=right> assign                                # ass
    |   <assoc=right> ID                                    # myid
    |   <assoc=right> query                                 # qry
    |   <assoc=right> slash                                 # slsh
    ;

assign:ID ':' expr;
multiAssign:(assign* (';' assign)*);
table:'(' '[' keycols=multiAssign ']' tcols=multiAssign ')';
multiID:(ID* (';' ID)*);
function:'{' ('[' multiID ']')? stat '}';

csvq: (expr?|(expr (',' expr)*));
query:'select' csel=csvq ('by' (by=expr (',' expr)*))? 'from' tbl=ID ('where' wc=csvq)?;
scmd:(~(NEWLINE))*;
slash:'\\' cmd=scmd;

// LONG/FLOAT are the default types the other atoms/list use postfixes to set type
datatype: (SYMBOL SYMBOL+) #symbolList
         | BOOLLIST #boolList | BOOL #bool
         | NUMLIST  #numList
         | NUMNUM   #num
         | CHARLIST #charList
         | '(' ')' #emptyList
         | CHAR #char | SYMBOL #symbol
         | DATELIST #dateList | DATE #date
         | MONTHLIST #monthList | MONTH #month
         | TIMELIST #timeList | TIME #time
         | BYT      #byt;


NEWLINE:[\r\n]+;
WS  :   (' '|'\t')+ -> skip;
COMMENT:'/*' .*? '*/' -> channel(HIDDEN);
LINE_COMMENT:'/' ~[\r\n]* -> channel(HIDDEN);

fragment NumTypeLetter:('h'|'i'|'j'|'e'|'f');
NUMLIST:NUM (' ' NUM)+ NumTypeLetter?;
NUMNUM:NUM NumTypeLetter?;
NUM:(([-])* Floaty|([-])* [0-9]+|Specials Letter?);

BOOL:[01] 'b';
BOOLLIST: [10]+ 'b';
DATELIST:DAT (' ' DAT)+ ('d'|'n'|'p')?;
DATE:DAT ('d'|'n'|'p')?;
MONTH:MON 'm';
MONTHLIST:MON (' ' MON)+ 'm'?;
DAT:(MON '.' [0-9] [0-9]) ('D'|'D' TIM)?;
MON:Specials | ([0-9] [0-9] [0-9] [0-9] '.' [01] [0-9]);
// Should we only parse valid times?
TIMELIST:TIM (' ' TIM)+ ('n'|'u'|'v'|'t')?;
TIME:TIM ('n'|'u'|'v'|'t')?;
TIM:Specials | DD | DD? TI | (DD? TI  ':' [0-9] [0-9]) | (DD? TI ':' [0-9] [0-9] '.' Digits?);
DD:([-])* Digits 'D';
TI:([-])* [0-9] [0-9] ':' [0-9] [0-9];


MONOP:('til'|'enlist'|'first'|'last'|'distinct'|'count'|'key'|'where'|'reverse'|'null'|'not'
    |'raze'|'rotate'|'show'|'system'
    |'desc'|'asc'|'++'|'--'|'til'|'get'|'abs'|'all'|'any'|'avg'|'avgs'|'exp'|'floor'|'ceiling'
    |'cos'|'sin'|'tan'|'acos'|'asin'|'atan'|'exp'|'log'|'fills'|'flip'
    |'mcount'|'type'|'attr'|'reciprocal'|'sqrt'
    |'svar'|'sdev'|'var'|'dev'|'differ'|'getenv'|'group'|'iasc'
    |'idesc'|'::'
    |'max'|'maxs'|'min'|'mins'|'med'|'mmu'|'read0'|'read1'|'prd'|'prds'|'exit'|'neg'|'inv'
    |'rand'|'ratios'|'ratios'|'signum'|'value'
    |'trim'|'rtrim'|'ltrim'|'upper'|'lower'|'string'
    |'hcount'|'hdel'|'hsym'|'hopen'|'hclose'
    |'gtime'|'ltime'|'parse'|'views'|'tables'); // exotic
fragment Adverbs:('each'|'\\:'|'/:'|'\':'|'/'|'\\'|'peach');
BINOP:(':'|'<'|'>'|'<='|'>='|'&'|'|'|'~'|'?'|'='|'<>'|'+:'|'!'|'*:'|'^'|'+'|'-'|'*'|'%'|'@'|'and'|'or'|'set'|'mod'
    |'sublist'|'$'|'_'|'#'|','|'div'|'except'|'inter'|'union'|'deltas'|'cut'|'cross'|'bin'|'binr'
    |'in'|'within'|'insert'|'wsum'|'wavg'|'xexp'|'setenv'
    |'like'|'ss'|'sv'|'vs' // string
    |'mdev'|'mmax'|'mmin'|'msum'  // moving
    |'aj'|'ej'|'ij'|'lj'|'pj'|'uj'|'wj' // joins
    |'.')
    |Adverbs;
ID:	[a-zA-Z_.] [a-zA-Z0-9_.]*;

SYMBOL: ('`' (Letter|Digit|'_'|'.')*) | ('`:' (Letter|Digit|'_'|'.'|'/')*);

CHAR: '"' (~('"' | '\r' | '\n') | '\\' ('"' | '\\')) '"';
CHARLIST:'"' (~('"' | '\r' | '\n') | '\\' ('"' | '\\'))* '"';
BYT: '0' 'x' [0-9a-fA-F]*;

Digits:Digit (Digit)*;
Digit: '0' | [1-9];

fragment Floaty:	Digits '.' Digits? ExponentPart?
	|	'.' Digits ExponentPart?
	|	Digits ExponentPart;

fragment ExponentPart: [eE] [+-]? Digits;
fragment Letter:[a-zA-Z_];
fragment LetterOrDigit:	[a-zA-Z0-9_];
fragment Specials: ('0N' | '0W' | '-0W' | '0n' | '0w' | '-0w');


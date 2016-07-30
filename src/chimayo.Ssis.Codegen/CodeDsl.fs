namespace Chimayo.Ssis.CodeGen.CodeDsl

type CodeTree =
    | EmptyCodeTree // Used to support pattern matching but not supported for generation
    | Namespace of string * CodeTree list
    | Open of string
    | ModuleAlias of string * string
    | Module of string * CodeTree list
    | LetBinding of bool * string * string option * CodeTree list * CodeTree * CodeTree option
    | ListExpression of CodeTree list
    | RecordExpression of (string * CodeTree) list
    | RecordMutationExpression of CodeTree * ((string*CodeTree) list)
    | FunctionApplication of CodeTree * CodeTree list
    | Constant of ConstantValue
    | Parentheses of bool * CodeTree
    | UnaryOp of bool * string*CodeTree
    | BinaryOp of string*CodeTree*CodeTree
    | TypedExpression of CodeTree * string
    | NamedValue of string
    | InlineExpression of CodeTree
    | Pipeline of string*CodeTree list
    | ManualLineBreak of CodeTree
    | BlankLine
    | Yield of bool * CodeTree
    | Tuple of CodeTree list

and ConstantValue =
    | Int32 of int
    | String of string
    | Float of float
    | Boolean of bool
    | DateTime of System.DateTime
    | DateTimeOffset of System.DateTimeOffset
    | Int8 of sbyte
    | UInt8 of byte
    | Int16 of int16
    | UInt16 of uint16
    | UInt32 of uint32
    | Int64 of int64
    | UInt64 of uint64
    | Float32 of float32
    | Decimal of decimal



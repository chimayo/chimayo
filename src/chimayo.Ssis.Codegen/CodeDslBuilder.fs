module Chimayo.Ssis.CodeGen.CodeDsl.Builder

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.CodeGen
open Chimayo.Ssis.CodeGen.CodeDsl
open Chimayo.Ssis.Ast.ControlFlow

let internal codename<'a> = 
    let rec codename' (t:System.Type) =
        if t.DeclaringType = null then t.FullName
        else
        sprintf "%s.%s" (codename' t.DeclaringType) t.Name
    codename' typedefof<'a>

let internal codename1<'a> = sprintf "%s.%s" (codename<'a>)

let inline constant x =
    let x' = 
        match x:>obj with
        | :? float as f -> Float f
        | :? float32 as f -> Float32 f
        | :? int as i -> Int32 i
        | :? string as s -> String s
        | :? bool as b -> Boolean b
        | :? System.DateTime as d -> DateTime d
        | :? System.DateTimeOffset as d -> DateTimeOffset d
        | :? decimal as d -> Decimal d
        | :? int8 as i -> Int8 i
        | :? int16 as i -> Int16 i
        | :? int64 as i -> Int64 i
        | :? uint8 as i -> UInt8 i
        | :? uint16 as i -> UInt16 i
        | :? uint32 as i -> UInt32 i
        | :? uint64 as i -> UInt64 i
        | _ -> failwith "Unsupported conversion to constant"
    Constant x'

let parentheses b x = Parentheses (b,x)
let maybeParenthesise b = parentheses b @?@ id
let functionApplication name args = FunctionApplication (NamedValue name,args)
let functionApplicationQ func args = functionApplication (QuotationHelper.getFullyQualifiedFunctionNameFromLambda func) args
let apply f args = FunctionApplication (f,args)
let pipelineOp op steps =
    match steps with
    | [] -> Pipeline (op, [])
    | [ct] -> ct
    | cts -> Pipeline (op, cts)
let pipeline = pipelineOp "|>"

let joinPipelineOp op ct pipe =
    match pipe with
    | Pipeline (op',cts) when op = op' -> pipelineOp op (cts @ [ct])
    | ct -> pipelineOp op [pipe ; ct]

let joinPipeline = joinPipelineOp "|>"

let makeOption addParentheses fn value = 
    let fn' = addParentheses |> (parentheses false) @?@ id
    match value with
    | None -> InlineExpression (FunctionApplication (NamedValue "None", [])) 
    | Some x -> InlineExpression (FunctionApplication (NamedValue "Some", [ fn x ]) |> fn')

let inline makeSome addParentheses value = makeOption addParentheses id (Some value)

let inline constantOption<'a> addParentheses = makeOption addParentheses constant
let boolOption addParentheses = makeOption addParentheses (Boolean >> Constant)
let intOption addParentheses = makeOption addParentheses (Int32 >> Constant)
let stringOption addParentheses = makeOption addParentheses (String >> Constant)

let inline defaultPrinter<'a> = sprintf "%A"

let inline qualifiedEnum<'a> prefix v = sprintf "%s.%s" prefix (defaultPrinter v) |> NamedValue
let inline fullyQualifiedEnum<'a> v = qualifiedEnum (TypeNameRegistry.typeToName v) v

let inline fullyQualifiedUnionCaseNoData<'a> v = sprintf "%s.%s" (TypeNameRegistry.innerTypeToName v) (v.GetType().Name) |> NamedValue
let inline fullyQualifiedUnionSingleCaseNoData<'a> v = v.GetType().Name |> NamedValue
let inline fullyQualifiedUnionCaseWithData<'a> v data = FunctionApplication (fullyQualifiedUnionCaseNoData v , data) 
let inline fullyQualifiedUnionSingleCaseWithData<'a> v data = FunctionApplication (fullyQualifiedUnionSingleCaseNoData v , data) 

let inline buildFlagsEnum<'a,'b when 'a : enum<'b>> (v:'a) =
    let definedValues = System.Linq.Enumerable.Cast<'a>(System.Enum.GetValues(typedefof<'a>)) |> Seq.toList
    let definedValuesInt = definedValues |> List.map (box >> unbox >> int32)
    let lookup = List.zip definedValuesInt definedValues |> Map.ofList
    let enumName = v.GetType().FullName |> TypeNameRegistry.aliasName
    let formatter x = sprintf "%s.%s" enumName (x.ToString()) |> NamedValue
    
    let rawValue = v |> box |> unbox |> int32

    if (rawValue = 0) then
        Map.tryFind 0 lookup |> function None -> constant 0 | Some x -> formatter x
    else

    let factors =
        definedValuesInt
        |> List.sortWith (fun x y -> compare (y |> int) (x |> int))
        |> List.fold 
                (fun (factors, value) defValue -> 
                    let dv = defValue |> int
                    match value with
                    | 0 -> (factors, value)
                    | _ when (value &&& dv) = dv -> (dv::factors, value - dv)
                    | _ when dv <= 0 -> (value::factors, 0)
                    | _ -> (factors, value)
                ) ([], rawValue)
        |> fun (factors, value) -> if value <> 0 then value::factors else factors
    
    factors
    |> List.map (fun v -> Map.tryFind v lookup |> function None -> constant v | Some x -> formatter x)
    |> pipelineOp "+"





let expression (CfExpression x) =
    FunctionApplication (TypeNameRegistry.nameLookup "CfExpression" |> constant , [constant x])

let dataValueSupportsDirectConstruction x =
    match x with
    | CfData.Empty -> Some (NamedValue "()")
    | CfData.Null -> Some (NamedValue "None")
    | CfData.Int8 v -> Some (constant v)
    | CfData.Int16 v -> Some (constant v)
    | CfData.Int32 v -> Some (constant v)
    | CfData.Int64 v -> Some (constant v)
    | CfData.UInt8 v -> Some (constant v)
    | CfData.UInt16 v -> Some (constant v)
    | CfData.UInt32 v -> Some (constant v)
    | CfData.UInt64 v -> Some (constant v)
    | CfData.Real32 v -> Some (constant v)
    | CfData.Real64 v -> Some (constant v)
    | CfData.Currency v -> Some (constant v)
    | CfData.Date v -> Some (constant v)
    | CfData.String v -> Some (constant v)
    | CfData.Boolean v -> Some (constant v)
    | CfData.Object -> None
    | CfData.Decimal v -> Some (constant v)
    | CfData.Guid v -> functionApplication "System.Guid" [ constant v ] |> Some
    | CfData.Numeric v -> None

let dataValueMaybeDirect includeCreateCall x = 
    let z = codename<CfData>
    let build0 name = 
        NamedValue
            (sprintf "%s.%s" codename<CfData> name)
    let build name x = 
        InlineExpression 
            (functionApplication (sprintf "%s.%s" codename<CfData> name) [ x ])
    let useCreate x = InlineExpression (functionApplication (sprintf "%s.%s" codename<CfData> "create") [ x ])
    let useCreate' = includeCreateCall |> useCreate @?@ id
    match dataValueSupportsDirectConstruction x, x with
    | Some ct, _ -> false, ct |> useCreate'
    | _, CfData.Currency v -> true, constant v |> build "Currency"
    | _, CfData.Object -> true, build0 "Object"
    | _, CfData.Numeric v -> true, constant v |> build "Numeric"
    | _ -> failwith "Unexpected case"

let dataValue includeCreateCall x = x |> dataValueMaybeDirect includeCreateCall |> snd

let functionApplicationWithDataValue includeCreateCall (directForm, prefixArgsDirect, suffixArgsDirect) (indirectForm, prefixArgsIndirect, suffixArgsIndirect) dataValue =
    let isDirect, value = dataValueMaybeDirect includeCreateCall dataValue
    let fn,p,s = isDirect |> (directForm, prefixArgsDirect, suffixArgsDirect) @?@ (indirectForm, prefixArgsIndirect, suffixArgsIndirect)
    let value' = isDirect |> (value |> parentheses false) @?@ value
    functionApplication fn [ yield! p; yield value' ; yield! s ]

let functionApplicationWithDataValueQ includeCreateCall (directForm, prefixArgsDirect, suffixArgsDirect) (indirectForm, prefixArgsIndirect, suffixArgsIndirect) dataValue =
    let isDirect, value = dataValueMaybeDirect includeCreateCall dataValue
    let fn,p,s = isDirect |> (directForm, prefixArgsDirect, suffixArgsDirect) @?@ (indirectForm, prefixArgsIndirect, suffixArgsIndirect)
    let value' = isDirect |> (value |> parentheses false) @?@ value
    functionApplicationQ fn [ yield! p; yield value' ; yield! s ]

let functionApplicationWithDataValueSimple includeCreateCall directForm indirectForm =
    functionApplicationWithDataValue includeCreateCall (directForm, [], []) (indirectForm, [], [])

let functionApplicationWithDataValueSimpleQ includeCreateCall directForm indirectForm =
    functionApplicationWithDataValueQ includeCreateCall (directForm, [], []) (indirectForm, [], [])

let rec combineDeclaration letBinding code =
    match letBinding with
    | EmptyCodeTree -> code
    | LetBinding (recurse, name, resultTypeOption, patterns, ct, None) ->
        LetBinding (recurse, name, resultTypeOption, patterns, ct, Some code)
    | LetBinding (recurse, name, resultTypeOption, patterns, ct, Some continuation) ->
        LetBinding (recurse, name, resultTypeOption, patterns, ct, combineDeclaration continuation code |> Some)
    | _ -> failwith "Unable to combine code parts"

let combineDeclarations letBindings code =
    match letBindings with
    | [] -> code
    | [x] -> combineDeclaration x code
    | _ ->
        let letBinding = letBindings |> List.reduce combineDeclaration
        combineDeclaration letBinding code

let listExpression mapper coll =
    match coll with
    | [] -> NamedValue "[]" |> InlineExpression
    | _ -> coll |> List.map mapper |> ListExpression

let makePair parenthesise v1 v2 =
    Tuple[v1;v2] |> (parenthesise |> (parentheses false) @?@ id)

let extendRecordExpressions expressions recordExpression =
    match recordExpression with
    | RecordExpression re -> re @ expressions |> RecordExpression
    | _ -> failwith "Not a record expression"

let maybeMutateRecordExpression testValue expectedValue baseValueExpression mutationExpressions =
    ((testValue = expectedValue) || (mutationExpressions |> List.length |> (=) 0))
    |> baseValueExpression @?@ RecordMutationExpression (baseValueExpression, mutationExpressions)

module Chimayo.Ssis.CodeGen.Pipeline.Destination.OleDb

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators
open Chimayo.Ssis.CodeGen.Internals
open Chimayo.Ssis.CodeGen.CodeDsl
open Chimayo.Ssis.CodeGen.CodeDsl.Builder
open Chimayo.Ssis.CodeGen.Core
open Chimayo.Ssis.CodeGen.QuotationHelper
open Chimayo.Ssis.CodeGen.PipelineCore

open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Ast.DataFlowApi

let buildColumn (d : DfOleDbDestinationColumn) =
    let (DfInputColumnReference(_,DfName columnName)) = d.sourceColumn |> Option.get
    pipelineOp ","
        [
            d.externalName |> constant
            d.externalDataType |> datatype
            columnName |> constant
        ]
    |> InlineExpression

let buildColumnsBySource (source, columns) =
    let (DfOutputReference (DfComponentReference cname, DfName oname)) = source
    let columnExpr =
        match columns with
        | [_] -> columns |> listExpression buildColumn |> InlineExpression
        | _ -> columns |> listExpression buildColumn


    Yield 
        (true, 
            functionApplicationQ <@ OleDbDestination.define_columns_for_source @>
                [
                    cname |> constant
                    oname |> constant
                    columnExpr |> ManualLineBreak
                ])

let buildUnmappedColumn (column : DfOleDbDestinationColumn) =
    Yield
        (false,
            functionApplicationQ <@ OleDbDestination.define_column @> 
                [ 
                    column.externalName |> constant
                    column.externalDataType |> datatypeWithParenthesesIfNeeded
                    NamedValue "None"
                ])

let buildFastLoadOption (flo : DfOleDbDestinationFastLoadOption) =
    let makeOrdering (cname,ascending) = makePair false (constant cname) (constant ascending)
    match flo with
    | DfOleDbDestinationFastLoadOption.Ordering os -> apply (fullyQualifiedUnionCaseNoData flo) [ listExpression makeOrdering os ]
    | DfOleDbDestinationFastLoadOption.RowsPerBatch n -> apply (fullyQualifiedUnionCaseNoData flo) [ constant n ]
    | DfOleDbDestinationFastLoadOption.KilobytesPerBatch kb -> apply (fullyQualifiedUnionCaseNoData flo) [ constant kb ]
    | DfOleDbDestinationFastLoadOption.Tablock -> fullyQualifiedEnum flo
    | DfOleDbDestinationFastLoadOption.CheckConstraints -> fullyQualifiedEnum flo
    | DfOleDbDestinationFastLoadOption.FireTriggers -> fullyQualifiedEnum flo


let buildFastLoadSpec (fls : DfOleDbDestinationFastLoadSettings) =
    RecordExpression
        [
            "keepIdentity", fls.keepIdentity |> constant
            "keepNulls", fls.keepNulls |> constant
            "options", fls.options |> listExpression buildFastLoadOption
            "maxRowsPerCommit", fls.maxRowsPerCommit |> constant
        ]

let buildTarget (t : DfOleDbDestinationTarget) =
    match t with
    | DfOleDbDestinationTarget.TableOrView tname ->
        functionApplicationQ <@ OleDbDestination.set_target_table_or_view @> [ tname |> constant ]
    | DfOleDbDestinationTarget.TableOrViewVariable sv ->
        functionApplicationQ <@ OleDbDestination.set_target_table_or_view_variable @> [ sv |> makeScopedVariableReference |> parentheses false ]
    | DfOleDbDestinationTarget.SqlTarget sql ->
        functionApplicationQ <@ OleDbDestination.set_target_sql @> [ sql |> constant ]
    | DfOleDbDestinationTarget.FastLoadTableOrView (tname,fls) ->
        let { keepIdentity = ki ; keepNulls = kn ; maxRowsPerCommit = mr ; options = options } = fls
        functionApplicationQ <@ OleDbDestination.set_target_table_or_view_fast_load @>
            [
                tname |> constant
                ki |> constant
                kn |> constant
                mr |> constant
                options |> listExpression buildFastLoadOption |> ManualLineBreak
            ]
        |> InlineExpression
    | DfOleDbDestinationTarget.FastLoadTableOrViewVariable (sv,fls) ->
        let { keepIdentity = ki ; keepNulls = kn ; maxRowsPerCommit = mr ; options = options } = fls
        functionApplicationQ <@ OleDbDestination.set_target_table_or_view_variable_fast_load @>
            [
                sv |> makeScopedVariableReference |> parentheses false
                ki |> constant
                kn |> constant
                mr |> constant
                options |> listExpression buildFastLoadOption |> ManualLineBreak
            ]
        |> InlineExpression

let buildInputConnection (DfInputConnection (DfName _, DfOutputReference (DfComponentReference cname, DfName oname))) =
    functionApplicationQ <@ OleDbDestination.connect_input_by_name @> [ cname |> constant ; oname |> constant ] |> InlineExpression
    
let buildComponent (c : DfComponent) =
    let dummy = Chimayo.Ssis.Ast.DataFlowApi.OleDbDestination.create "dummy" []
    let mutate eqOp getterFunction setterFunction mapper =
        eqOp (dummy |> getterFunction) (c |> getterFunction)
        |> [] @?@ [ functionApplication (getFullyQualifiedFunctionNameFromLambda setterFunction) [c |> getterFunction |> mapper] ]

    let columnsBySource = 
        c 
        |> OleDbDestination.get_columns 
        |> Seq.groupBy (fun column -> column.sourceColumn |> function Some (DfInputColumnReference(x,_)) -> Some x | _ -> None)
    
    let columnsWithSource = columnsBySource |> Seq.where (fst >> Option.isSome) |> Seq.map (fun (l,r)-> l |> Option.get , r |> Seq.toList) |> Seq.toList
    let unmappedColumns = columnsBySource |> Seq.tryFind (fst >> Option.isNone) |> optionOrDefaultMap (snd >> Seq.toList) []

    let allColumns =
        [
            yield! columnsWithSource |> List.map buildColumnsBySource
            yield! unmappedColumns |> List.map buildUnmappedColumn
        ]

    functionApplicationQ <@ Chimayo.Ssis.Ast.DataFlowApi.OleDbDestination.create @>
        [
            c.name |> constant
            ListExpression
                [
                    yield! mutate stringCompareInvariant PipelineCommon.get_description <@ PipelineCommon.set_description @> constant
                    yield! mutate (=) PipelineCommon.get_locale_id <@ PipelineCommon.set_locale_id @> (makeOption true constant)
                    yield! mutate (=) PipelineCommon.get_validate_external_metadata <@ PipelineCommon.set_validate_external_metadata @> (makeOption true constant)
                    yield! mutate (=) PipelineCommon.get_uses_dispositions <@ PipelineCommon.set_uses_dispositions @> (makeOption true constant)

                    yield! mutate (=) OleDbDestination.get_connection <@ OleDbDestination.set_connection @> (function CfRef.ConnectionManagerRef r -> r |> constant | _ -> failwith "invalid")
                    yield! mutate (=) OleDbDestination.get_timeout_seconds <@ OleDbDestination.set_timeout_seconds @> constant
                    yield! mutate (=) OleDbDestination.get_always_use_default_codepage <@ OleDbDestination.set_always_use_default_codepage @> constant
                    yield! mutate (=) OleDbDestination.get_default_codepage <@ OleDbDestination.set_default_codepage @> constant
                    yield! mutate (=) OleDbDestination.get_error_row_disposition <@ OleDbDestination.set_error_row_disposition @> fullyQualifiedEnum
                    
                    yield c |> OleDbDestination.get_target |> buildTarget

                    yield functionApplicationQ <@ OleDbDestination.add_columns @> [ListExpression(allColumns)]

                    yield! c.inputConnections |> List.map buildInputConnection

                ]
        ]

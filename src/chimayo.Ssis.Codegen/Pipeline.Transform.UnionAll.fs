module Chimayo.Ssis.CodeGen.Pipeline.Transform.UnionAll

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

let buildInputColumn (DfUnionAllInputColumn (inputName, sourceColumn) as column) =
    makePair false (inputName |> DfNamedEntity.decode |> constant) (inputColumnReference false sourceColumn)

let buildColumn (c : DfUnionAllColumn) =
    functionApplicationQ <@ UnionAll.define_column @>
        [
            c.name |> DfNamedEntity.decode |> constant
            c.dataType |> datatypeWithParenthesesIfNeeded
            c.mappedInputColumns |> listExpression buildInputColumn
        ]
    |> fun ct -> CodeTree.Yield(false,ct)
    
let getInputs c = c.mappedInputColumns |> List.map (function DfUnionAllInputColumn (DfName i,_) -> i)

let getSourceColumns c = c.mappedInputColumns |> List.map (function DfUnionAllInputColumn (_, cref) -> cref)

let buildCommonColumn c =
    let sourceColumn = c |> getSourceColumns |> List.head 
    makePair false
        (sourceColumn |> inputColumnReference false)
        (c.dataType |> datatype)

let buildColumns (cs : DfUnionAllColumn list) =
    let inputs = cs |> List.collect getInputs |> Set.ofList
    let combination3 f g h v = (f v) && (g v) && (h v)

    let columnNameIsSameAsSourceColumnName c =
        let _,_,sourceColumnName = c |> getSourceColumns |> List.head |> DfInputColumnReference.decode
        c.name |> DfNamedEntity.decode |> stringCompareInvariant sourceColumnName

    let commonColumns, uniqueColumns =
        cs
        |> List.partition
            (combination3
                (getInputs >> Set.ofList >> (=) inputs) // present on all inputs
                (getSourceColumns >> Set.ofList >> Set.count >> (=) 1) // single source for each column
                columnNameIsSameAsSourceColumnName) // source column has the same name as the output column
    [
        yield functionApplicationQ <@ UnionAll.define_common_columns @>
                [
                    inputs |> Set.toList |> listExpression constant |> InlineExpression
                    commonColumns |> listExpression buildCommonColumn

                ]
              |> fun ct -> Yield(true, ct)
        
        yield! uniqueColumns |> List.map buildColumn
    ]



let buildInputConnection (DfInputConnection (DfName iname, DfOutputReference (DfComponentReference cname, DfName oname))) =
    functionApplicationQ <@ UnionAll.connect_input_by_name @> [ iname |> constant ; cname |> constant ; oname |> constant ] |> InlineExpression

let buildComponent (c : DfComponent) =
    let dummy = Chimayo.Ssis.Ast.DataFlowApi.UnionAll.create "dummy" []
    let mutate eqOp getterFunction setterFunction mapper =
        eqOp (dummy |> getterFunction) (c |> getterFunction)
        |> [] @?@ [ functionApplication (getFullyQualifiedFunctionNameFromLambda setterFunction) [c |> getterFunction |> mapper] ]

    functionApplicationQ <@ Chimayo.Ssis.Ast.DataFlowApi.UnionAll.create @>
        [
            c.name |> constant
            ListExpression
                [
                    yield! mutate stringCompareInvariant PipelineCommon.get_description <@ PipelineCommon.set_description @> constant
                    yield! mutate (=) PipelineCommon.get_locale_id <@ PipelineCommon.set_locale_id @> (makeOption true constant)
                    yield! mutate (=) PipelineCommon.get_validate_external_metadata <@ PipelineCommon.set_validate_external_metadata @> (makeOption true constant)
                    yield! mutate (=) PipelineCommon.get_uses_dispositions <@ PipelineCommon.set_uses_dispositions @> (makeOption true constant)
                
                    yield functionApplicationQ <@ UnionAll.add_columns @> [c |> UnionAll.get_columns |> buildColumns |> listExpression id]

                    yield! c.inputConnections |> List.map buildInputConnection
                ]
        ]
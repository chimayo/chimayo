module Chimayo.Ssis.CodeGen.Pipeline.Destination.Recordset

open Chimayo.Ssis.CodeGen.Internals
open Chimayo.Ssis.CodeGen.CodeDsl
open Chimayo.Ssis.CodeGen.CodeDsl.Builder
open Chimayo.Ssis.CodeGen.Core
open Chimayo.Ssis.CodeGen.PipelineCore

open Chimayo.Ssis.Ast.DataFlow

let buildColumn (d : DfRecordsetDestinationColumn) =
    RecordExpression
        [ 
            "readOnly", d.readOnly |> constant
            "sourceColumn", d.sourceColumn |> inputColumnReference false
        ]
        

let build (d : DfRecordsetDestinationConfiguration) =
    RecordExpression
        [
            "variable", d.variable |> makeScopedVariableReference
            "columns", d.columns |> listExpression buildColumn
        ]
    


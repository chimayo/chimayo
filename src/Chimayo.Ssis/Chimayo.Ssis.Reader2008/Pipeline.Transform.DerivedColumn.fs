module Chimayo.Ssis.Reader2008.Pipeline.Transform.DerivedColumn

open Chimayo.Ssis.Common
open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Ast.DataFlowApi
open Chimayo.Ssis.Reader2008
open Chimayo.Ssis.Reader2008.Internals
open Chimayo.Ssis.Reader2008.Common
open Chimayo.Ssis.Reader2008.Pipeline.Common

[<Literal>]
let classId = "{2932025B-AB99-40F6-B5B8-783A73F80E24}"

let readNewColumn nav : DfDerivedColumnColumn =
    let codepage = nav |> Extractions.anyInt "@codePage" Defaults.codePage
    let dt = nav |> Extractions.anyString "@dataType" ""
    let precision = nav |> Extractions.anyInt "@precision" 0
    let scale = nav |> Extractions.anyInt "@scale" 0
    let length = nav |> Extractions.anyInt "@length" 0
    let errorDisposition = nav |> Extractions.anyString "@errorRowDisposition" "" |> DfOutputColumnRowDisposition.fromString
    let truncationDisposition = nav |> Extractions.anyString "@truncationRowDisposition" "" |> DfOutputColumnRowDisposition.fromString

    let behaviour =
        DfDerivedColumnColumnBehaviour.NewColumn
            (
                nav |> Extractions.anyString "@name" "" |> DfName
              , buildPipelineDataType dt codepage precision scale length
              , nav |> Pipeline.Expressions.readExpression
            )
    {
        behaviour = behaviour
        errorRowDisposition = errorDisposition
        truncationRowDisposition = truncationDisposition
    }

let readReplaceColumn nav : DfDerivedColumnColumn =
    let codepage = nav |> Extractions.anyInt "@codePage" Defaults.codePage
    let errorDisposition = nav |> Extractions.anyString "@errorRowDisposition" "" |> DfOutputColumnRowDisposition.fromString
    let truncationDisposition = nav |> Extractions.anyString "@truncationRowDisposition" "" |> DfOutputColumnRowDisposition.fromString

    let lineageId = nav |> Extractions.anyString "@lineageId" ""
    let sourceColumn, sourceOutput, sourceComponent = 
        nav 
        |> select "ancestor::components/component/outputs/output/outputColumns/outputColumn[@id]"
        |> List.filter (fun nav' -> nav' |>  Extractions.anyString "@id" "" |> (fun s -> System.String.Equals(s, lineageId, System.StringComparison.InvariantCulture)))
        |> List.head
        |> fun nav' ->
                        nav' |> Extractions.anyString "@name" ""
                      , nav' |> Extractions.anyString "ancestor::output/@name" ""
                      , nav' |> Extractions.anyString "ancestor::component/@name" ""


    let behaviour =
        DfDerivedColumnColumnBehaviour.ReplaceColumn
            ( DfInputColumnReference.build sourceComponent sourceOutput sourceColumn
            , Pipeline.Expressions.readExpression nav
            )
    {
        behaviour = behaviour
        errorRowDisposition = errorDisposition
        truncationRowDisposition = truncationDisposition
    }


let read nav =
    let newColumns = 
        nav 
        |> navMap "outputs/output[not(@isErrorOut) or (@isErrorOut != 'true')]/outputColumns/outputColumn" readNewColumn 
    let replaceColumns =
        nav
        |> navMap "inputs/input/inputColumns/inputColumn[@usageType='readWrite']" readReplaceColumn

    (newColumns @ replaceColumns)
    |> fun cs -> ( { columns = cs } : DfDerivedColumnConfiguration) 
    |> DfDerivedColumn

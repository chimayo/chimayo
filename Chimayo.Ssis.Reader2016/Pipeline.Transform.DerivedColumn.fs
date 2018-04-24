module Chimayo.Ssis.Reader2016.Pipeline.Transform.DerivedColumn

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Ast.DataFlowApi
open Chimayo.Ssis.Reader2016.Internals
open Chimayo.Ssis.Reader2016.Common
open Chimayo.Ssis.Reader2016.Pipeline
open Chimayo.Ssis.Reader2016.Pipeline.Common

[<Literal>]
let classId = "{49928E82-9C4E-49F0-AABE-3812B82707EC}"

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
              , nav |> Expressions.readExpression
            )
    {
        behaviour = behaviour
        errorRowDisposition = errorDisposition
        truncationRowDisposition = truncationDisposition
    }

let readReplaceColumn nav : DfDerivedColumnColumn =
    let codepage = nav |> Extractions.anyInt "@codePage" Defaults.codePage
    let errorDisposition = nav |> Extractions.anyString "@errorRowDisposition" "NotUsed" |> DfOutputColumnRowDisposition.fromString
    let truncationDisposition = nav |> Extractions.anyString "@truncationRowDisposition" "NotUsed" |> DfOutputColumnRowDisposition.fromString

    let lineageId = nav |> Extractions.anyString "@lineageId" ""
    let sourceColumn, sourceOutput, sourceComponent = 
        nav 
        |> select "ancestor::components/component/outputs/output/outputColumns/outputColumn[@refId]"
        |> List.filter (fun nav' -> nav' |>  Extractions.anyString "@refId" "" |> (fun s -> System.String.Equals(s, lineageId, System.StringComparison.InvariantCulture)))
        |> List.head
        |> fun nav' ->
                        nav' |> Extractions.anyString "@name" ""
                      , nav' |> Extractions.anyString "ancestor::output/@name" ""
                      , nav' |> Extractions.anyString "ancestor::component/@name" ""


    let behaviour =
        DfDerivedColumnColumnBehaviour.ReplaceColumn
            ( DfInputColumnReference.build sourceComponent sourceOutput sourceColumn
            , nav |> Expressions.readExpression
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

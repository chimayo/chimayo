module Chimayo.Ssis.Reader2016.Pipeline.Destination.OleDb

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Ast.DataFlowApi
open Chimayo.Ssis.Reader2016.Internals
open Chimayo.Ssis.Reader2016.Common


open Chimayo.Ssis.Reader2016.Pipeline.Common

[<Literal>]
let classId = "{4ADA7EAA-136C-4215-8098-D7A7C27FC0D1}"

module FastLoadOptionParser =
    open FParsec

    let emptyParser = stringReturn "" []

    let skipOptionalWhitespace = skipAnyOf [' ';'\r';'\n';'\t'] |> optional
    let separator s = between skipOptionalWhitespace skipOptionalWhitespace s
    let separatorChar c = separator <| skipChar c

    let parseIntOption option fn =
        pstring option
        >>. skipOptionalWhitespace 
        >>. skipChar '=' 
        >>. skipOptionalWhitespace 
        >>. pint32 
        >>= (fn >> preturn)
    
    let parseTablock = stringReturn "TABLOCK" DfOleDbDestinationFastLoadOption.Tablock
    let parseCheckConstraints = stringReturn "CHECK_CONSTRAINTS" DfOleDbDestinationFastLoadOption.CheckConstraints
    let parseFireTriggers = stringReturn "FIRE_TRIGGERS" DfOleDbDestinationFastLoadOption.FireTriggers
    let parseRowsPerBatch = parseIntOption "ROWS_PER_BATCH" DfOleDbDestinationFastLoadOption.RowsPerBatch
    let parseKilobytesPerBatch = parseIntOption "KILOBYTES_PER_BATCH" DfOleDbDestinationFastLoadOption.KilobytesPerBatch
    let parseOrderingColumn =
        preturn ("",false)
    let parseOrdering =
        skipString "ORDER"
        >>. between (skipOptionalWhitespace >>. skipChar '(' >>. skipOptionalWhitespace) (skipOptionalWhitespace >>. skipChar ')' >>. skipOptionalWhitespace)
                (sepBy parseOrderingColumn (separatorChar ',') >>= (DfOleDbDestinationFastLoadOption.Ordering >> preturn)
                )
    let parseOne =
            parseTablock
        <|> parseCheckConstraints
        <|> parseFireTriggers
        <|> parseRowsPerBatch
        <|> parseKilobytesPerBatch
    
    let parse : Parser<DfOleDbDestinationFastLoadOption list,_> =
            (sepBy parseOne (separatorChar ',') .>> eof)
        <|> (emptyParser .>> eof)
        
    let read (options:string) =
        match FParsec.CharParsers.run parse (options.ToUpperInvariant()) with
        | Success (options',_,_) -> options'
        | Failure (errormsg,_,_) -> failwith errormsg


let readTarget nav =
    let accessMode = nav |> readPipelinePropertyInt "AccessMode" -1
    let rowset = nav |> readPipelinePropertyString "OpenRowset" ""
    let rowsetvar = nav |> readPipelinePropertyString "OpenRowsetVariable" "" |> CfVariableRef.fromString
    let sqlcmd = nav |> readPipelinePropertyString "SqlCommand" ""
    let flidentity = nav |> readPipelinePropertyBool "FastLoadKeepIdentity" false
    let flnulls = nav |> readPipelinePropertyBool "FastLoadKeepNulls" false
    let floptions = nav |> readPipelinePropertyString "FastLoadOptions" "" |> FastLoadOptionParser.read
    let flmaxcommit = nav |> readPipelinePropertyInt "FastLoadMaxInsertCommitSize" 0
    let flsettings : DfOleDbDestinationFastLoadSettings =
        {
            keepIdentity  = flidentity
            keepNulls = flnulls
            options = floptions
            maxRowsPerCommit = flmaxcommit
        }
    match accessMode with
    | 0 -> DfOleDbDestinationTarget.TableOrView rowset
    | 1 -> DfOleDbDestinationTarget.TableOrViewVariable rowsetvar
    | 2 -> DfOleDbDestinationTarget.SqlTarget sqlcmd
    | 3 -> DfOleDbDestinationTarget.FastLoadTableOrView (rowset, flsettings)
    | 4 -> DfOleDbDestinationTarget.FastLoadTableOrViewVariable (rowsetvar, flsettings)
    | _ -> failwith "invalid access mode"


let readMetaDataColumn nav =
    let refId = nav |> Extractions.anyString "@refId" ""
    let codepage = nav |> Extractions.anyInt "@codePage" Defaults.codePage
    let dt = nav |> Extractions.anyString "@dataType" ""
    let precision = nav |> Extractions.anyInt "@precision" 0
    let scale = nav |> Extractions.anyInt "@scale" 0
    let length = nav |> Extractions.anyInt "@length" 0
    let name = nav |> Extractions.anyString "@name" ""
    refId , (name, buildPipelineDataType dt codepage precision scale length)


let readColumn metadataMap nav =
    let sourceColumnLineageIdNav = nav |> Extractions.anyString "@lineageId" "" |> (swap lookupLineageId) nav
    let sourceColumn, sourceOutput, sourceComponent =
        sourceColumnLineageIdNav |> Extractions.anyString "@name" ""
        , sourceColumnLineageIdNav |> Extractions.anyString "ancestor::output/@name" ""
        , sourceColumnLineageIdNav |> Extractions.anyString "ancestor::component/@name" ""
    let metaDataRefId = nav |> Extractions.anyString "@externalMetadataColumnId" ""
    let metadataName, metadataDt = metadataMap |> Map.find metaDataRefId
    {
        externalName = metadataName
        externalDataType = metadataDt
        sourceColumn = DfInputColumnReference.build sourceComponent sourceOutput sourceColumn |> Some
    }
    
let toMetadataOnlyColumn (name,dt) =
    {
        externalName = name
        externalDataType = dt
        sourceColumn = None
    }
    

let readColumns nav =
    let metaData = 
        nav 
        |> navMap "inputs/input/externalMetadataColumns/externalMetadataColumn" readMetaDataColumn
        |> Map.ofList 
    
    let inputColumns = 
        nav |> navMap "inputs/input/inputColumns/inputColumn" (readColumn metaData)
    let mappedColumns =
        inputColumns |> List.map (fun x -> x.externalName) |> Set.ofList
    let remainingColumns =
        metaData 
        |> Map.toSeq 
        |> Seq.map (snd) 
        |> Seq.filter (fst >> mappedColumns.Contains >> not)
        |> Seq.map toMetadataOnlyColumn

    remainingColumns |> appendSeqToList inputColumns
    
let read nav =
    DfOleDbDestination
        {
            timeoutSeconds = nav |> readPipelinePropertyInt "CommandTimeout" 0
            target = nav |> readTarget
            alwaysUseDefaultCodePage = nav |> readPipelinePropertyBool "AlwaysUseDefaultCodePage" false
            defaultCodePage = nav |> readPipelinePropertyInt "DefaultCodePage" Defaults.codePage
            connection = nav |> readSingularConnectionManagerRef
            columns = nav |> readColumns
            errorRowDisposition = nav |> getValueOrDefault "inputs/input/@errorRowDisposition" DfOutputColumnRowDisposition.fromString DfOutputColumnRowDisposition.FailComponent
        }

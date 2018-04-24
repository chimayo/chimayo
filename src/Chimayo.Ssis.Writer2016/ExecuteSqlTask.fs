module Chimayo.Ssis.Writer2016.Executables.ExecuteSqlTask


open Chimayo.Ssis.Writer2016.DtsIdMonad
open Chimayo.Ssis.Xml.Dsl

open Chimayo.Ssis.Writer2016
open Chimayo.Ssis.Writer2016.Core
open Chimayo.Ssis.Writer2016.Executables.TaskCommon

open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi


let ns = "www.microsoft.com/sqlserver/dts/tasks/sqltask"
let prefix = "SQLTask"
let nsp' = (ns,prefix)::defaultNamespacesAndPrefixes

let createSqlElement localName = XmlElement.create localName ns nsp'
let createSqlAttribute localName = XmlAttribute.create localName ns
let createSimpleSqlElement localName value = createSqlElement localName |> XmlElement.setContent [ Text value ]

let decodeSourceType = function
    | CfIndirectSource (CfRef.ConnectionManagerRef _) -> "FileConnection"
    | CfIndirectSource (CfRef.VariableRef _) -> "Variable"
    | CfDirectSource _ -> "DirectInput"
    | _ -> failwith "Invalid SQL statement source"

let decodeDirection = function
    | CfParameterDirection.InputParameter -> "Input"
    | CfParameterDirection.OutputParameter -> "Output"
    | CfParameterDirection.ReturnValue -> "ReturnValue"

let buildParameterBindings bindings =
    let buildParameterBinding (pb ) =
        createSqlElement "ParameterBinding"
        |> XmlElement.setAttributes
            [
                yield pb 
                        |> ExecuteSql.ParameterBindings.getParameterName 
                        |> createSqlAttribute "ParameterName" 
                yield pb
                        |> ExecuteSql.ParameterBindings.getTargetVariable 
                        |> Variables.scopedReferenceToString 
                        |> createSqlAttribute "DtsVariableName" 
                yield pb
                        |> ExecuteSql.ParameterBindings.getDirection
                        |> decodeDirection
                        |> createSqlAttribute "ParameterDirection"
                yield pb
                        |> ExecuteSql.ParameterBindings.getDataType 
                        |> (int) |> (string) 
                        |> createSqlAttribute "DataType"
                yield! pb 
                        |> ExecuteSql.ParameterBindings.getParameterSize 
                        |> Option.toList 
                        |> List.map (string >> createSqlAttribute "ParameterSize")
           ]
    bindings |> List.map buildParameterBinding

let buildSourceValue (t:CftExecuteSql) =
    dtsIdState {
            match t.source with
            | CfIndirectSource x -> 
                match x with
                | CfRef.ConnectionManagerRef _ -> 
                    let! _, connRefId = x |> ConnectionManager.getNameFromReference |> RefIds.getConnectionManagerIds
                    return connRefId |> guidWithBraces
                | CfRef.VariableRef x -> return x.``namespace`` + "::" + x.name
                | _ -> return failwith "Invalid source specification"
            | CfDirectSource x -> return x
               }

let buildResult (t:CftExecuteSql) =
    let resultType =
        t.resultType 
        |> function 
            | CfExecuteSqlResult.NoResult -> "ResultSetType_None"
            | CfExecuteSqlResult.RowsetResult -> "ResultSetType_Rowset"
            | CfExecuteSqlResult.SingleRowResult -> "ResultSetType_SingleRow"
            | CfExecuteSqlResult.XmlResult -> "ResultSetType_XML"
        |> createSqlAttribute "ResultType" 

    let buildBinding (resultName, variableRef) =
        createSqlElement "ResultBinding"
        |> XmlElement.setAttributes
            [
                resultName |> createSqlAttribute "ResultName"
                variableRef |> Variables.scopedReferenceToString |> createSqlAttribute "DtsVariableName"
            ]

    resultType, t.resultBindings |> List.map buildBinding
               

let buildSqlData (t:CftExecuteSql) =
    dtsIdState {

        let! _,connId = t.connection |> ConnectionManager.getNameFromReference |> RefIds.getConnectionManagerIds 
        let! sourceValue = buildSourceValue t
        let sourceType = decodeSourceType t.source
        let parameterBindings = buildParameterBindings t.parameterBindings
        let resultType, resultBindings = buildResult t

        return
            createSqlElement "SqlTaskData"
            |> XmlElement.setAttributes
                [
                    yield createSqlAttribute "Connection" (connId |> guidWithBraces)
                    yield createSqlAttribute "TimeOut" (t.timeoutSeconds |> (string))
                    yield createSqlAttribute "IsStoredProc" (t.isStoredProc |> boolToTitleCase)
                    yield createSqlAttribute "BypassPrepare" (t.bypassPrepare |> boolToTitleCase)
                    yield createSqlAttribute "CodePage" (t.codePage |> (string))
                    yield resultType
                    yield createSqlAttribute "SqlStmtSourceType" sourceType
                    yield createSqlAttribute "SqlStatementSource" sourceValue
                ]
            |> XmlElement.setContent
                [
                    yield! parameterBindings
                    yield! resultBindings
                ]
                }

let build parents (t:CftExecuteSql) =
    dtsIdState {
        
        let! elem =
            getBasicTaskElement 
                "Microsoft.SqlServer.Dts.Tasks.ExecuteSQLTask.ExecuteSQLTask, Microsoft.SqlServer.SQLTask, Version=13.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91" 
                parents 
                t.executableTaskBase

        let! sqlData = buildSqlData t

        let elem' =
            elem
            |> XmlElement.addContent
                [
                    yield (sqlData |> DefaultElements.objectData)
                ]

        return elem'

               }


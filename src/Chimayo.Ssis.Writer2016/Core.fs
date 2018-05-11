module Chimayo.Ssis.Writer2016.Core

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.Dsl
open Chimayo.Ssis.Writer2016.DtsIdMonad
open Microsoft.FSharp.Collections
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi

let namespaceDts = "www.microsoft.com/SqlServer/Dts"
let prefixDts = "DTS"

let defaultNamespacesAndPrefixes = 
    [
         namespaceDts, prefixDts
         "", ""
    ]


module RefIds =
    let assignAndGetIds refIdGenerator value =
        dtsIdState {
                        let refId = value |> refIdGenerator 
                        let! dtsId = refId |> DtsIdState.getDtsId
                        return refId, dtsId
                   }
    let tryLookupId refIdGenerator value =
        dtsIdState {
                        let refId = value |> refIdGenerator 
                        let! dtsId = refId |> DtsIdState.tryGetDtsId
                        return refId, dtsId
                   }

    let getConnectionManagerRefId name = "Package.ConnectionManagers[" + name + "]"
    let getConnectionManagerIds = assignAndGetIds getConnectionManagerRefId
    let getVariableRefId parents (ns, name) = 
        sprintf "%s.Variables[%s::%s]" (System.String.Join("\\", List.rev (parents))) ns name

    let getVariableIds parents = assignAndGetIds (getVariableRefId parents)

    let rec tryGetVariableIdsRecurse parents (ns,name) =
        dtsIdState {
            let! result = tryLookupId (getVariableRefId parents) (ns,name) 
            match result |> snd, parents with
            | Some x, _ -> return Some x
            | _, [] -> return None
            | _, p::ps -> return! tryGetVariableIdsRecurse ps (ns,name)
                   }
    
    let getParameterRefId parents (ns, name) = 
        sprintf "%s.Parameters[%s::%s]" (System.String.Join("\\", List.rev (parents))) ns name

    let getParameterIds parents = assignAndGetIds (getParameterRefId parents)

    let rec tryGetParameterIdsRecurse parents (ns,name) =
        dtsIdState {
            let! result = tryLookupId (getParameterRefId parents) (ns,name) 
            match result |> snd, parents with
            | Some x, _ -> return Some x
            | _, [] -> return None
            | _, p::ps -> return! tryGetParameterIdsRecurse ps (ns,name)
                   }


    let getLogProviderRefId name = "Package.LogProviders[" + name + "]"
    let getLogProviderIds = assignAndGetIds getLogProviderRefId

    let getExecutableRefId parents name = System.String.Join("\\", List.rev (name::parents))
    let getExecutableIds parents = assignAndGetIds (getExecutableRefId parents)

    let getPrecedenceConstraintRefId parents fromExecutableName toExecutableName =
      sprintf "%s.PrecedenceConstraints[%s  <--  %s]"
            (System.String.Join("\\", List.rev parents))
            toExecutableName
            fromExecutableName

    let getForEachEnumeratorGuids parents =
      dtsIdState {
          let path = (System.String.Join("\\", List.rev parents))
          let refId, objName = sprintf "%s.ForEachEnumerator.RefId" path , sprintf "%s.ForEachEnumerator.ObjectName" path
          let! refIdGuid = DtsIdState.getDtsId refId
          let! objNameGuid = DtsIdState.getDtsId objName
          return objNameGuid , refIdGuid
                 }
    let getForEachEnumeratorVariableGuid parents variableName =
      let path = (System.String.Join("\\", List.rev parents))
      let refId = sprintf "%s.ForEachEnumerator.VariableMapping[%s]" path variableName
      DtsIdState.getDtsId refId
    
    let getForEachEnumeratorParameterGuid parents parameterName =
      let path = (System.String.Join("\\", List.rev parents))
      let refId = sprintf "%s.ForEachEnumerator.ParameterMapping[%s]" path parameterName
      DtsIdState.getDtsId refId

    let getPipelineComponentRefId parents name = System.String.Join("\\", List.rev (name::parents))
    let getPipelineComponentIds parents = assignAndGetIds (getPipelineComponentRefId parents)

    let getPipelineConnectionRefId parents name = sprintf "%s.Connections[%s]" (getPipelineComponentRefId (List.tail parents) (List.head parents)) name
    let getPipelineConnectionIds parents = assignAndGetIds (getPipelineConnectionRefId parents)

    let getPipelineInputRefId parents name = sprintf "%s.Inputs[%s]" (getPipelineComponentRefId (List.tail parents) (List.head parents)) name
    let getPipelineInputIds parents = assignAndGetIds (getPipelineInputRefId parents)

    let getPipelineOutputRefId parents name = sprintf "%s.Outputs[%s]" (getPipelineComponentRefId (List.tail parents) (List.head parents)) name
    let getPipelineOutputIds parents = assignAndGetIds (getPipelineOutputRefId parents)

    let getPipelineOutputColumnRefId parents outputName name = sprintf "%s.Columns[%s]" (getPipelineOutputRefId parents outputName) name
    let getPipelineOutputColumnIds parents outputName = assignAndGetIds (getPipelineOutputColumnRefId parents outputName) 

    let getPipelineInputColumnRefId parents inputName name = sprintf "%s.Columns[%s]" (getPipelineInputRefId parents inputName) name
    let getPipelineInputColumnIds parents inputName = assignAndGetIds (getPipelineInputColumnRefId parents inputName) 

    let getPipelineMetadataInputColumnRefId parents inputName name = sprintf "%s.ExternalColumns[%s]" (getPipelineInputRefId parents inputName) name
    let getPipelineMetadataInputColumnIds parents inputName = assignAndGetIds (getPipelineMetadataInputColumnRefId parents inputName) 

    let getPipelineMetadataOutputColumnRefId parents outputName name = sprintf "%s.ExternalColumns[%s]" (getPipelineOutputRefId parents outputName) name
    let getPipelineMetadataOutputColumnIds parents outputName = assignAndGetIds (getPipelineMetadataOutputColumnRefId parents outputName) 

    let getUniquePipelinePathRefId parents name = sprintf "%s.Paths[%s]" (System.String.Join("\\", List.rev (parents))) name
    let getUniquePipelinePathIds parents = assignAndGetIds (getUniquePipelinePathRefId parents)

let createElement localName = XmlElement.create localName "" defaultNamespacesAndPrefixes
let createAttribute localName = XmlAttribute.create localName ""
let removeAttribute localName = XmlElement.removeAttribute localName ""
let createSimpleElement localName value = createElement localName |> XmlElement.setContent [ Text value ]
let createDtsElement localName = XmlElement.create localName namespaceDts defaultNamespacesAndPrefixes
let createDtsAttribute localName = XmlAttribute.create localName namespaceDts

let createDtsSimpleElement localName value = createDtsElement localName |> XmlElement.setContent [ Text value ]

let createDtsPropertyElement propertyName value =
    createDtsElement "Property"
    |> XmlElement.setAttributes [ createDtsAttribute "Name" propertyName ]
    |> XmlElement.setContent [ XmlContent.Text value ]

let guidWithBraces (x:System.Guid) = 
    x.ToString("B").ToUpperInvariant()

let boolToTitleCase (b:bool) = if b then "True" else "False"
let boolToLowerCase (b:bool) = if b then "true" else "false"
let boolToUpperCase (b:bool) = if b then "TRUE" else "FALSE"

let now = fun () -> System.DateTimeOffset.UtcNow

let dataValueToString value =
    match value with
        | CfData.Empty -> ""
        | CfData.Null -> ""
        | CfData.Int8 v -> System.Convert.ToString(v)
        | CfData.Int16 v -> System.Convert.ToString(v)
        | CfData.Int32 v -> System.Convert.ToString(v)
        | CfData.Int64 v -> System.Convert.ToString(v)
        | CfData.UInt8 v -> System.Convert.ToString(v)
        | CfData.UInt16 v -> System.Convert.ToString(v)
        | CfData.UInt32 v -> System.Convert.ToString(v)
        | CfData.UInt64 v -> System.Convert.ToString(v)
        | CfData.Real32 v -> System.Convert.ToString(v)
        | CfData.Real64 v -> System.Convert.ToString(v)
        | CfData.Currency v -> System.Convert.ToString(v)
        | CfData.Date v -> failwith "Export of date values is not implemented"
        | CfData.String v -> v
        | CfData.Boolean v -> v |> "-1" @?@ "0"
        | CfData.Object -> ""
        | CfData.Decimal v -> System.Convert.ToString(v)
        | CfData.Guid v -> v |> guidWithBraces
        | CfData.Numeric v -> System.Convert.ToString(v)


module DefaultProperties =
    let creationDate (dt:System.DateTimeOffset) =
        createDtsAttribute "CreationDate" (dt.ToString("G", System.Globalization.CultureInfo("en-US")))

    let forceExecutionValue forcedValue =
        match forcedValue with
        | Some x -> [
                      createDtsAttribute "ForceExecValue" (true |> boolToTitleCase)
                      createDtsAttribute "ExecValue" (x |> dataValueToString)
                      createDtsAttribute "ExecValueType" (x |> CfData.getType |> (int) |> (string))
                    ]
        | None ->  [ ]


    let forceExecutionResult (forcedResult : CfExecutableResult option) =
        match forcedResult with
        | Some x -> [ createDtsAttribute "ForceExecutionResult" (x |> (int) |> (string)) ]
        | None ->  [ ]

        
    let refId = createDtsAttribute "refId" 

    let localeId = (string) >> createDtsAttribute "LocaleID" 

    let dtsId = guidWithBraces >> createDtsAttribute "DTSID" 

    let objectName value = 
        match value with
        | "" -> createDtsAttribute "ObjectName" (System.Guid.NewGuid().ToString("P"))
        | _ -> createDtsAttribute "ObjectName" value

    let creationName = createDtsAttribute "CreationName" 

    let maxConcurrentExecutables = (string) >> createDtsAttribute "MaxConcurrentExecutables" 

    let delayValidation = boolToTitleCase >> createDtsAttribute "DelayValidation" 

    let bypassPrepare = boolToTitleCase >> createDtsAttribute "BypassPrepare" 

    let description = createDtsAttribute "Description" 

    let disabled = boolToTitleCase >> createDtsAttribute "Disabled" 

    let disableEventHandlers = boolToTitleCase >> createDtsAttribute "DisableEventHandlers"

    let failParentOnFailure = boolToTitleCase >> createDtsAttribute "FailParentOnFailure"
    let failPackageOnFailure = boolToTitleCase >> createDtsAttribute "FailPackageOnFailure"
    let failOnErrorCountReaching =  (string) >> createDtsAttribute "MaxErrorCount"

    let isolationLevel (isolationLevel : CfIsolationLevel) = isolationLevel |> (int) |> (string) |> createDtsAttribute "ISOLevel"
    let transactionOption (transactionOption : CfTransactionOption) = transactionOption |> (int) |> (string) |> createDtsAttribute "TransactionOption"

module DefaultElements =
    open Chimayo.Ssis.Writer2016.DtsIdMonad

    let objectData elem =
        createDtsElement "ObjectData"
        |> XmlElement.setContent [ elem ]
        
    let buildExpression expr =
        let target = expr |> Expressions.getTargetProperty
        let value = expr |> Expressions.getExpressionPropertyText
        createDtsElement "PropertyExpression"
        |> XmlElement.setAttributes [ yield createDtsAttribute "Name" target ]
        |> XmlElement.setContent [ yield Text value ]

    let buildExpressions exprs = exprs |> List.sortBy Expressions.getTargetProperty |>  List.map buildExpression

    let buildProperty name content = createDtsElement "Property" |> XmlElement.setAttributes [ createDtsAttribute "Name" name ] |> XmlElement.setContent content

    let buildVariable assigner var =
        dtsIdState {
            let ns, name = var |> Variables.getQualifiedName 
            let! _, dtsId = (ns, name) |> assigner

            let nullRefData =
                let nsp =
                    [
                        "http://schemas.xmlsoap.org/soap/envelope/" , "SOAP-ENV"
                        "http://www.w3.org/2001/XMLSchema" , "xsd"
                    ]
                XmlElement.create "Envelope" "http://schemas.xmlsoap.org/soap/envelope/" nsp
                |> XmlElement.setAttributes [ XmlAttribute.create "encodingStyle" "http://schemas.xmlsoap.org/soap/envelope/" "http://schemas.xmlsoap.org/soap/encoding/" ]
                |> XmlElement.setContent
                    [
                        XmlElement.create "Body" "http://schemas.xmlsoap.org/soap/envelope/" nsp
                        |> XmlElement.setContent
                            [
                                XmlElement.create "anyType" "http://www.w3.org/2001/XMLSchema" nsp
                                |> XmlElement.setAttributes
                                    [
                                        XmlAttribute.create "id" "" "ref-1"
                                    ]
                            ]
                    ]

            let subTypeAttrs, valueContent = 
                match var.value with
                | CfData.Object ->
                    [ createDtsAttribute "DataSubType" "ManagedSerializable" ] , [nullRefData]
                | _ -> 
                    let valueString = var.value |> dataValueToString
                    [], System.String.IsNullOrEmpty(valueString) |> [] @?@ [valueString |> Text]


            return 
                createDtsElement "Variable"
                |> XmlElement.setAttributes
                    [
                        yield DefaultProperties.objectName name
                        yield createDtsAttribute "Namespace" ns
                        yield DefaultProperties.dtsId dtsId
                        yield! var.expression |> Option.toList |> List.map (Expressions.getExpressionText >> createDtsAttribute "Expression")
                        yield var.expression |> Option.isSome |> boolToTitleCase |> createDtsAttribute "EvaluateAsExpression"
                        yield var.isReadOnly |> boolToTitleCase |> createDtsAttribute "ReadOnly"
                    ]
                |> XmlElement.setContent 
                    [ 
                        yield createDtsElement "VariableValue"
                              |> XmlElement.setAttributes 
                                    [
                                       yield var.value |> CfData.getType |> (int) |> (string) |> createDtsAttribute "DataType"
                                    ]
                              |> XmlElement.addAttributes subTypeAttrs
                              |> XmlElement.setContent valueContent
                    ]

                   }

    let buildVariables parents vars =

        dtsIdState {
            let mapper = parents |> RefIds.getVariableIds |> buildVariable
            let! content = vars |> DtsIdState.listmap mapper
            return createDtsElement "Variables" |> XmlElement.setContent [ yield! content ]
                   }

    let buildParameter assigner par =
        dtsIdState {
            let ns, name = par |> Parameters.getQualifiedName 
            let! _, dtsId = (ns, name) |> assigner

            let nullRefData =
                let nsp =
                    [
                        "http://schemas.xmlsoap.org/soap/envelope/" , "SOAP-ENV"
                        "http://www.w3.org/2001/XMLSchema" , "xsd"
                    ]
                XmlElement.create "Envelope" "http://schemas.xmlsoap.org/soap/envelope/" nsp
                |> XmlElement.setAttributes [ XmlAttribute.create "encodingStyle" "http://schemas.xmlsoap.org/soap/envelope/" "http://schemas.xmlsoap.org/soap/encoding/" ]
                |> XmlElement.setContent
                    [
                        XmlElement.create "Body" "http://schemas.xmlsoap.org/soap/envelope/" nsp
                        |> XmlElement.setContent
                            [
                                XmlElement.create "anyType" "http://www.w3.org/2001/XMLSchema" nsp
                                |> XmlElement.setAttributes
                                    [
                                        XmlAttribute.create "id" "" "ref-1"
                                    ]
                            ]
                    ]

            let subTypeAttrs, valueContent = 
                match par.value with
                | CfData.Object ->
                    [ createDtsAttribute "DataSubType" "ManagedSerializable" ] , [nullRefData]
                | _ -> 
                    let valueString = par.value |> dataValueToString
                    [], System.String.IsNullOrEmpty(valueString) |> [] @?@ [valueString |> Text]

            let parameterValue = [ createDtsAttribute "Name" "ParameterValue" ]

            return 
                createDtsElement "PackageParameter"
                |> XmlElement.setAttributes
                    [
                        yield DefaultProperties.objectName name
                        yield DefaultProperties.dtsId dtsId
                        yield par.isRequired |> boolToTitleCase |> createDtsAttribute "Required"
                        //yield par.isSensitive |> boolToTitleCase |> createDtsAttribute "Sensitive"
                    ]
                |> XmlElement.setContent 
                    [ 
                        yield createDtsElement "Property"
                              |> XmlElement.setAttributes 
                                    [
                                       yield par.value |> CfData.getType |> (int) |> (string) |> createDtsAttribute "DataType"
                                    ]
                              |> XmlElement.addAttributes parameterValue
                              //|> XmlElement.addAttributes subTypeAttrs
                              |> XmlElement.setContent valueContent
                    ]

                   }

    let buildParameters parents pars =

        dtsIdState {
            let mapper = parents |> RefIds.getParameterIds |> buildParameter
            let! content = pars |> DtsIdState.listmap mapper
            return createDtsElement "PackageParameters" |> XmlElement.setContent [ yield! content ]
                   }

    let buildLoggingOptions (loggingOptions : CfLoggingOptions) =
        
        let buildSelectedLogProvider provider =
            dtsIdState {
                let! _, guid = provider |> RefIds.getLogProviderIds
                return createDtsElement "SelectedLogProvider"
                    |> XmlElement.setAttributes
                        [
                            guid |> guidWithBraces |> createDtsAttribute "InstanceID"
                        ]
                       }

        let rec buildSelectedLogProviders () =
            dtsIdState {
                
                let! lps = loggingOptions.logProviders |> DtsIdState.listmap buildSelectedLogProvider
                let lps' = lps |> fun els -> els.IsEmpty |> None @?@ Some els
                let mapper data = createDtsElement "SelectedLogProviders" |> XmlElement.setContent [ yield! data ]

                return lps' |> Option.map mapper |> Option.toList

                       }

        dtsIdState {

            let eventFilterProperty = 
                let create data =
                    createDtsElement "Property"
                    |> XmlElement.setAttributes
                        [
                            yield createDtsAttribute "DataType" (CfDataType.String |> (int) |> (string))
                            yield createDtsAttribute "Name" "EventFilter"
                        ]
                    |> XmlElement.setContent [ Text data ]
                loggingOptions.logSelections 
                |> List.map (fun ls -> sprintf "%d,%s" ls.eventName.Length ls.eventName)
                |> function
                    | [] -> None
                    | coll -> 
                        (string) coll.Length :: coll
                        |> List.toArray
                        |> fun arr -> System.String.Join(",", arr)
                        |> create
                        |> Some
                |> Option.toList
                    
            let columnFilterProperties =
                let createProperty data =
                    createDtsElement "Property"
                    |> XmlElement.setAttributes [ createDtsAttribute "Name" data ]
                    |> XmlElement.setContent [ Text "-1" ]
                let createColumnFilter (data : CfLogEventSettings) = 
                    createDtsElement "Property"
                    |> XmlElement.setAttributes
                        [
                            createDtsAttribute "EventName" data.eventName
                            createDtsAttribute "Name" "ColumnFilter"
                        ]
                    |> XmlElement.setContent (data.columns |> List.sort |> List.map createProperty)
                loggingOptions.logSelections
                |> List.sortBy (fun (t : CfLogEventSettings) -> t.eventName)
                |> List.map createColumnFilter
            
            let! selectedLogProviders = buildSelectedLogProviders ()

            let elem =
                createDtsElement "LoggingOptions"
                |> XmlElement.setAttributes
                    [
                        yield loggingOptions.loggingMode |> (int) |> (string) |> createDtsAttribute "LoggingMode"
                        yield loggingOptions.filterKind |> (int) |> (string) |> createDtsAttribute "FilterKind"
                    ]
                |> XmlElement.setContent
                    [
                        yield! eventFilterProperty
                        yield! columnFilterProperties
                        yield! selectedLogProviders
                    ]

            return elem
                   }

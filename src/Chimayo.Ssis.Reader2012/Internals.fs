module Chimayo.Ssis.Reader2012.Internals

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow

module XmlNs =
    let dtsNamespace = "www.microsoft.com/SqlServer/Dts"
    let dtsPrefix = "DTS"

    let sqlTaskNamespace = "www.microsoft.com/sqlserver/dts/tasks/sqltask"
    let sqlTaskPrefix = "SQLTask"

    let defaultNsPrefix = 
        [
            dtsPrefix, dtsNamespace
            sqlTaskPrefix, sqlTaskNamespace
        ]

let (|XPathSelect1|_|) expr nav = Navigator.activepattern_xpathselect1 XmlNs.defaultNsPrefix expr nav
let (|XPathSelect|) expr nav = Navigator.activepattern_xpathselect XmlNs.defaultNsPrefix expr nav
let (|XPathValueString|_|) expr nav = Navigator.activepattern_xpathvaluestring XmlNs.defaultNsPrefix expr nav

let (|XPathSelect1V|_|) expr vars nav = Navigator.activepattern_xpathselect1v XmlNs.defaultNsPrefix expr vars nav
let (|XPathSelectV|) expr vars nav = Navigator.activepattern_xpathselectv XmlNs.defaultNsPrefix expr vars nav
let (|XPathValueStringV|_|) expr vars nav = Navigator.activepattern_xpathvaluestringv XmlNs.defaultNsPrefix expr vars nav

let (|CheckMatchMany|_|) matchList nav =
    matchList |> List.tryFind (fun xpath -> match nav with | XPathSelect1 xpath _ -> true | _ -> false)


let select1 xpath = function | XPathSelect1 xpath nav -> nav | _ -> failwith "Not found"
let select xpath = function | XPathSelect xpath nodes -> nodes
let maybeSelect1 xpath = function | XPathSelect1 xpath nav -> Some nav | _ -> None

let select1V xpath vars = function | XPathSelect1V xpath vars nav -> nav | _ -> failwith "Not found"
let selectV xpath vars = function | XPathSelectV xpath vars nodes -> nodes
let maybeSelect1V xpath vars = function | XPathSelect1V xpath vars nav -> Some nav | _ -> None

let normaliseLineEndings =
    let re = System.Text.RegularExpressions.Regex("(?<!\r)\n")
    fun x -> re.Replace(x, System.Environment.NewLine)

let getValueString xpath = function | XPathValueString xpath value -> value |> normaliseLineEndings |> Some | _ -> None
let getValueStringOrError xpath = function | XPathValueString xpath value -> value | _ -> failwith (sprintf "XPath '%s' was not matched" xpath)
let getValueOrDefault xpath mapper defValue nav = getValueString xpath nav |> Option.fold (fun _ v -> mapper v) defValue

let navMap xpath mapper = function | XPathSelect xpath nodes -> List.map mapper nodes


module Extractions =
    type Extractor<'a> = NavigatorRec -> 'a

    type estring = Extractor<string>
    type eint = Extractor<int>
    type ebool = Extractor<bool>
    type edatetimeoffset = Extractor<System.DateTimeOffset>

    let anyInt xpath = getValueOrDefault xpath (int)
    let anyIntOption xpath = getValueOrDefault xpath (int >> Some) None
    let anyBool xpath = getValueOrDefault xpath System.Convert.ToBoolean
    let anyBoolOption xpath = getValueOrDefault xpath (System.Convert.ToBoolean >> Some) None
    let anyBoolFromMinusOneOrZeroStringOrError xpath = getValueStringOrError xpath >> ((int) >> ((=) -1))
    let anyString xpath = getValueOrDefault xpath id
    let anyStringOption xpath = getValueOrDefault xpath (fun s -> if System.String.IsNullOrEmpty(s) then None else Some s) None
    let inline anyIntEnum xpath = getValueOrDefault xpath stringInt32ToEnum
    let inline anyStringEnum xpath = getValueOrDefault xpath stringToEnum
    
    let refId : estring = getValueStringOrError "self::*/@DTS:refId"
    let creationDate : edatetimeoffset = getValueStringOrError "self::*/@DTS:CreationDate" >> fun s -> System.DateTimeOffset.Parse(s, System.Globalization.CultureInfo("en-US"))
    let objectName : estring = getValueStringOrError "self::*/@DTS:ObjectName"
    let objectNameOrEmpty : estring = getValueOrDefault "self::*/@DTS:ObjectName" id ""
    let description : estring = anyString "self::*/@DTS:Description" ""
    let delayValidation : ebool = anyBool "self::*/@DTS:DelayValidation" false
    let localeId : eint = anyInt "self::*/@DTS:LocaleID" System.Globalization.CultureInfo.InstalledUICulture.LCID
    let disabled : ebool = anyBool "self::*/@DTS:Disabled" false
    let disableEventHandlers : ebool = anyBool "self::*/@DTS:DisableEventHandlers" false
    let failParentOnFailure : ebool = anyBool "self::*/@DTS:FailParentOnFailure" false
    let failPackageOnFailure : ebool = anyBool "self::*/@DTS:FailPackageOnFailure" false
    let failOnErrorCountReaching : eint = anyInt "self::*/@DTS:MaxErrorCount" 1
    let isolationLevel : Extractor<CfIsolationLevel> = 
        getValueOrDefault "self::*/@DTS:ISOLevel" (int >> enum<CfIsolationLevel>) CfIsolationLevel.Serializable
    let transactionOption : Extractor<CfTransactionOption> =
        getValueOrDefault "self::*/@DTS:TransactionOption" (int >> enum<CfTransactionOption>) CfTransactionOption.Supported
    

    let readDataValue valueType xpath nav =
        let value = nav |> anyString xpath ""
        let boolParse = function "-1" -> true | "0" -> false | _ -> failwith "Invalid bool value"
        match valueType with
        | CfDataType.Empty -> CfData.Empty
        | CfDataType.Null -> CfData.Null
        | CfDataType.Int8 -> CfData.Int8 (value |> (int8))
        | CfDataType.Int16 -> CfData.Int16 (value |> (int16))
        | CfDataType.Int32 -> CfData.Int32 (value |> (int32))
        | CfDataType.Int64 -> CfData.Int64 (value |> (int64))
        | CfDataType.UInt8 -> CfData.UInt8 (value |> (uint8))
        | CfDataType.UInt16 -> CfData.UInt16 (value |> (uint16))
        | CfDataType.UInt32 -> CfData.UInt32 (value |> (uint32))
        | CfDataType.UInt64 -> CfData.UInt64 (value |> (uint64))
        | CfDataType.Real32 -> CfData.Real32 (value |> (float32))
        | CfDataType.Real64 -> CfData.Real64 (value |> (float))
        | CfDataType.Currency -> CfData.Currency (value |> (decimal))
        | CfDataType.Date -> CfData.Date (System.Convert.ToDateTime(value, System.Globalization.CultureInfo.GetCultureInfo("en-US")))
        | CfDataType.String -> CfData.String value
        | CfDataType.Boolean -> CfData.Boolean (value |> boolParse)
        | CfDataType.Decimal -> CfData.Decimal (value |> (decimal))
        | CfDataType.Guid -> CfData.Guid (System.Guid(value))
        | CfDataType.Numeric -> CfData.Numeric (value |> (decimal))
        | CfDataType.Object ->
            if System.String.IsNullOrWhiteSpace(value) |> not then
                eprintf "WARNING: Value of type 'Object' is discarded (value: '%s')" value
            CfData.Object
        | _ -> failwith (sprintf "Data type %A is not supported" valueType)
            
    let readForceExecutionResult nav =
        nav
        |> anyInt "self::*/@DTS:ForceExecutionResult" -1 
        |> function 
           | -1 -> None 
           | x -> x |> enum<CfExecutableResult> |> Some

    let readForcedExecutionValue nav =
      nav 
      |> anyBool "self::*/@DTS:ForceExecValue" false
      |> function | false -> None 
                  | _ -> 
                      let dt = nav |> anyIntEnum "self::*/@DTS:ExecValueType" CfDataType.Empty
                      nav |> readDataValue dt "self::*/@DTS:ExecValue" |> Some


module Defaults =
    let codePage = System.Text.Encoding.Default.WindowsCodePage

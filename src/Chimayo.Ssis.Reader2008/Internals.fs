module Chimayo.Ssis.Reader2008.Internals

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

let (|CheckMatchManyValues|_|) caseSensitive matchList value =
    let tester = caseSensitive |> cond stringCompareInvariant stringCompareInvariantIgnoreCase
    matchList |> List.fold (fun s v -> if s then true else tester value v) false |> cond (Some true) None

let select1 xpath = function | XPathSelect1 xpath nav -> nav | _ -> failwith "Not found"
let select xpath = function | XPathSelect xpath nodes -> nodes
let maybeSelect1 xpath = function | XPathSelect1 xpath nav -> Some nav | _ -> None

let select1V xpath vars = function | XPathSelect1V xpath vars node -> node | _ -> failwith "Not found"
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
    let anyBoolFromMinusOneOrZeroString xpath = getValueOrDefault xpath ((int) >> ((=) -1))
    let anyBoolFromOneOrZeroStringOrError xpath = getValueStringOrError xpath >> ((int) >> ((=) 1))
    let anyBoolFromOneOrZeroString xpath = getValueOrDefault xpath ((int) >> ((=) 1))
    let anyString xpath = getValueOrDefault xpath id
    let anyStringOption xpath = getValueOrDefault xpath (fun s -> if System.String.IsNullOrEmpty(s) then None else Some s) None
    let inline anyIntEnum xpath = getValueOrDefault xpath stringInt32ToEnum
    let inline anyStringEnum xpath = getValueOrDefault xpath stringToEnum
    
    let namedPropertyTextXPath propertyName = sprintf "self::*/DTS:Property[@DTS:Name='%s']/text()" propertyName

    module PropertyElement =

        let getValueString name = getValueString (namedPropertyTextXPath name)
        let getValueStringOrError name = getValueStringOrError (namedPropertyTextXPath name)
        let getValueOrDefault name = getValueOrDefault (namedPropertyTextXPath name)

        let anyInt name = anyInt (namedPropertyTextXPath name)
        let anyIntOption name = anyIntOption (namedPropertyTextXPath name)
        let anyBool name = anyBool (namedPropertyTextXPath name)
        let anyBoolOption name = anyBoolOption (namedPropertyTextXPath name)
        let anyBoolFromMinusOneOrZeroString name = anyBoolFromMinusOneOrZeroString (namedPropertyTextXPath name)
        let anyBoolFromMinusOneOrZeroStringOrError name = anyBoolFromMinusOneOrZeroStringOrError (namedPropertyTextXPath name)
        let anyBoolFromOneOrZeroString name = anyBoolFromOneOrZeroString (namedPropertyTextXPath name)
        let anyBoolFromOneOrZeroStringOrError name = anyBoolFromOneOrZeroStringOrError (namedPropertyTextXPath name)
        let anyString name = anyString (namedPropertyTextXPath name)
        let anyStringOption name = anyStringOption (namedPropertyTextXPath name)
        let inline anyIntEnum name = anyIntEnum (namedPropertyTextXPath name)
        let inline anyStringEnum name = anyStringEnum (namedPropertyTextXPath name)


    let dtsId : estring = PropertyElement.getValueStringOrError "DTSID"
    let refId : estring = getValueStringOrError "self::*/@DTS:refId"
    let creationDate : edatetimeoffset = PropertyElement.getValueStringOrError "CreationDate" >> fun s -> System.DateTimeOffset(System.DateTime.Parse(s, System.Globalization.CultureInfo("en-US")))
    let objectName : estring = PropertyElement.getValueStringOrError "ObjectName"
    let objectNameOrEmpty : estring = PropertyElement.getValueOrDefault "ObjectName" id ""
    let description : estring = PropertyElement.anyString "Description" ""
    let delayValidation : ebool = PropertyElement.anyBoolFromOneOrZeroString "DelayValidation" false
    let localeId : eint = PropertyElement.anyInt "LocaleID" System.Globalization.CultureInfo.InstalledUICulture.LCID
    let disabled : ebool = PropertyElement.anyBoolFromOneOrZeroString "Disabled" false
    let disableEventHandlers : ebool = PropertyElement.anyBoolFromMinusOneOrZeroString "DisableEventHandlers" false
    let failParentOnFailure : ebool = PropertyElement.anyBoolFromOneOrZeroString "FailParentOnFailure" false
    let failPackageOnFailure : ebool = PropertyElement.anyBoolFromOneOrZeroString "FailPackageOnFailure" false
    let failOnErrorCountReaching : eint = PropertyElement.anyInt "MaxErrorCount" 1
    let isolationLevel : Extractor<CfIsolationLevel> = 
        PropertyElement.getValueOrDefault "ISOLevel" (int >> enum<CfIsolationLevel>) CfIsolationLevel.Serializable
    let transactionOption : Extractor<CfTransactionOption> =
        PropertyElement.getValueOrDefault "TransactionOption" (int >> enum<CfTransactionOption>) CfTransactionOption.Supported
    

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
        |> PropertyElement.anyInt "ForceExecutionResult" -1 
        |> function 
           | -1 -> None 
           | x -> x |> enum<CfExecutableResult> |> Some

    let readForcedExecutionValue nav =
      nav 
      |> PropertyElement.anyBoolFromMinusOneOrZeroString "ForceExecValue" false
      |> function | false -> None 
                  | _ -> 
                      let dt = nav |> PropertyElement.anyIntEnum "ExecValueType" CfDataType.Empty
                      nav |> readDataValue dt (namedPropertyTextXPath "ExecValue") |> Some


module Defaults =
    let codePage = System.Text.Encoding.Default.WindowsCodePage

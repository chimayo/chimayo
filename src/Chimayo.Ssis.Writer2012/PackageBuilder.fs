module Chimayo.Ssis.Writer2012.PackageBuilder

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators
open Chimayo.Ssis.Writer2012.DtsIdMonad
open Chimayo.Ssis.Xml.Dsl
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi

open Chimayo.Ssis.Writer2012
open Chimayo.Ssis.Writer2012.Core
open Chimayo.Ssis.Writer2012.Executables.TaskCommon

let buildSqlLogProvider (slp : CfSqlLogProvider) =
    dtsIdState {
        let! refId,guid = RefIds.getLogProviderIds slp.name

        return createDtsElement "LogProvider"
            |> XmlElement.setAttributes
                [
                    yield DefaultProperties.dtsId guid
                    yield DefaultProperties.creationName "DTS.LogProviderSQLServer.3"
                    yield DefaultProperties.delayValidation slp.delayValidation
                    yield DefaultProperties.description "Writes log entries for events to a SQL Server database"
                    yield DefaultProperties.objectName slp.name
                    yield createDtsAttribute "ConfigString" (slp.connection |> ConnectionManager.getNameFromReference)
                ]
            |> XmlElement.setContent
                [
                    yield 
                        createElement "InnerObject"
                        |> DefaultElements.objectData
                ]
               }

let buildLogProvider lp =
    match lp with
    | CfSqlLogProvider slp -> buildSqlLogProvider slp

let buildLogProviders lps =
    dtsIdState {
        let! lps = lps |> DtsIdState.listmap buildLogProvider
        return createDtsElement "LogProviders" |> XmlElement.setContent lps
               }

let buildParentVariableConfiguration (config : CfParentVariablePackageConfiguration) =
      dtsIdState {
        let v = config.ignoreParentVariableNamespace |> config.parentVariableName.name @?@ (config.parentVariableName |> CfVariableRef.toString)
        
        let es = config.expressions |> DefaultElements.buildExpressions

        let! dtsId = DtsIdState.getDtsId (sprintf "\Package.Configurations[%s]" config.name)

        return 
            createDtsElement "Configuration"
            |> XmlElement.setAttributes
                [
                    DefaultProperties.creationName ""
                    DefaultProperties.dtsId dtsId
                    DefaultProperties.objectName config.name
                    createDtsAttribute "ConfigurationString" v
                    createDtsAttribute "ConfigurationVariable" config.targetProperty
                ]
            |> XmlElement.setContent es
                 }
let buildConfiguration config =
    match config with
    | CfParentVariablePackageConfiguration pvc -> buildParentVariableConfiguration pvc

let buildConfigurations configs =
      dtsIdState {
    let! configs = configs |> List.sortBy PackageConfigurations.getName |> DtsIdState.listmap buildConfiguration
    return createDtsElement "Configurations" |> XmlElement.setContent configs
                 }

let buildPackage registrations (pkg:CftPackage) =
    let pkg =
        dtsIdState {

            do! DtsIdState.register registrations

            let refId = "Package"
            let! cms = pkg.connectionManagers |> ConnectionManagers.buildConnectionManagers
            let! vs = pkg.variables |> DefaultElements.buildVariables ["Package"]
            let es = pkg.propertyExpressions |> DefaultElements.buildExpressions
            let! exs = pkg.executables |> Executables.TopLevel.buildExecutables ["Package"]
            let! pcs = buildPrecedenceConstraints ["Package"] pkg.executables
            let! lo = pkg.loggingOptions |> DefaultElements.buildLoggingOptions
            let! lps = pkg.logProviders |> buildLogProviders
            let! configs = pkg.configurations |> buildConfigurations

            let creationDate = pkg.creationDate |> optionOrDefault (now())

            let! dtsId = DtsIdState.getDtsId refId
    
            let elem =
                createDtsElement "Executable"
                |> XmlElement.setContent
                        [
                            yield createDtsPropertyElement "PackageFormatVersion" "6"
                            yield cms
                            yield configs
                            yield lps
                            yield vs
                            yield lo
                            yield! es
                            yield exs
                            yield pcs
                        ]
                |> XmlElement.setAttributes
                        [
                            yield DefaultProperties.objectName pkg.name
                            yield createDtsAttribute "ExecutableType" "SSIS.Package.3"
                            yield createDtsAttribute "ProtectionLevel" "0"
                            yield DefaultProperties.maxConcurrentExecutables pkg.maxConcurrentExecutables
                            yield createDtsAttribute "EnableConfig" (pkg.enableConfigurations |> boolToTitleCase)
                            yield DefaultProperties.dtsId dtsId
                            yield DefaultProperties.creationDate creationDate
                            yield DefaultProperties.refId refId
                            yield DefaultProperties.localeId pkg.localeId
                            yield! DefaultProperties.forceExecutionValue pkg.forcedExecutionValue
                            yield! DefaultProperties.forceExecutionResult pkg.forcedExecutionResult
                            yield DefaultProperties.disabled pkg.disabled
                            yield DefaultProperties.disableEventHandlers pkg.disableEventHandlers
                            yield DefaultProperties.failParentOnFailure pkg.failParentOnFailure
                            yield DefaultProperties.failOnErrorCountReaching pkg.failOnErrorCountReaching
                            yield DefaultProperties.isolationLevel pkg.isolationLevel
                            yield DefaultProperties.transactionOption pkg.transactionOption
                            yield DefaultProperties.delayValidation pkg.delayValidation
                            yield createDtsAttribute "PackageType" "5"
                        ]


//            let! x = DtsIdMonad.Internals.getState
//            printf "\n%A\n\n" x
            
            let! registrations' = DtsIdState.getRegistrations
            return elem,registrations'
        }
    pkg.Execute()
    
    
let generate = PackageNormaliser.normalise >> buildPackage [] >> fst >> XmlDocument.create
let generateI registrations pkg = 
  let dtsx,registrations' = pkg |> PackageNormaliser.normalise |> buildPackage registrations
  dtsx |> XmlDocument.create , registrations'

let toXmlDocument = generate >> Chimayo.Ssis.Xml.Generator.generate
let toXmlDocumentI registrations pkg =
  let dtsx,registrations' = generateI registrations pkg
  dtsx |> Chimayo.Ssis.Xml.Generator.generate , registrations'

let toString = toXmlDocument >> fun d -> use sw = new System.IO.StringWriter() in d.Save(sw) ; sw.Flush() ; sw.ToString()
let save filename = toXmlDocument >> fun d -> d.Save(filename : string)
let saveI filename registrations pkg =
  let doc,registrations' = toXmlDocumentI registrations pkg
  doc |> fun d -> d.Save(filename : string)
  registrations'

    
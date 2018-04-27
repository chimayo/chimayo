module Chimayo.Ssis.Reader2016.Package

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Reader2016.Internals

let read (nav:NavigatorRec) : CftPackage =
    { 
        name = nav |> Extractions.objectName
        creationDate = nav |> Extractions.creationDate |> Some
        
        connectionManagers = nav |> ConnectionManagers.read
        configurations = nav |> PackageConfigurations.read
        logProviders = nav |> LogProviders.read
        variables = nav |> Variables.read
        loggingOptions = nav |> LoggingOptions.read
        propertyExpressions = nav |> Common.readPropertyExpressions
        executables = nav |> Executables.read

        maxConcurrentExecutables = nav |> Extractions.anyInt "self::*/@DTS:MaxConcurrentExecutables" -1
        enableConfigurations = nav |> Extractions.anyBool "self::*/@DTS:EnableConfig" false

        forcedExecutionValue = nav |> Extractions.readForcedExecutionValue
        forcedExecutionResult = nav |> Extractions.readForceExecutionResult
        disableEventHandlers = nav |> Extractions.disableEventHandlers
        
        disabled = nav |> Extractions.disabled
        
        failParentOnFailure = nav |> Extractions.failParentOnFailure
        failOnErrorCountReaching = nav |> Extractions.failOnErrorCountReaching

        isolationLevel = nav |> Extractions.isolationLevel
        localeId = nav |> Extractions.localeId
        transactionOption = nav |> Extractions.transactionOption
        delayValidation = nav |> Extractions.delayValidation
        parameters = nav |> Parameters.read
    }

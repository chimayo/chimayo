module Chimayo.Ssis.Reader2012.ExecutableTaskBase

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Reader2012.Internals

let read nav : CftBase =
    {
        description = nav |> Extractions.description
        name= nav |> Extractions.objectName
        expressions= nav |> Common.readPropertyExpressions

        variables= nav |> Variables.read
        loggingOptions= nav |> LoggingOptions.read
        precedenceConstraints= nav |> PrecedenceConstraints.read (nav |> Extractions.refId)

        forcedExecutionValue = nav |> Extractions.readForcedExecutionValue
        forcedExecutionResult = nav |> Extractions.readForceExecutionResult
        disabled= nav |> Extractions.disabled
        failPackageOnFailure= nav |> Extractions.failPackageOnFailure
        failParentOnFailure= nav |> Extractions.failParentOnFailure
        failOnErrorCountReaching= nav |> Extractions.failOnErrorCountReaching
        isolationLevel= nav |> Extractions.isolationLevel
        localeId= nav |> Extractions.localeId
        transactionOption= nav |> Extractions.transactionOption
        delayValidation= nav |> Extractions.delayValidation
        disableEventHandlers = nav |> Extractions.disableEventHandlers
    }


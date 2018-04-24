module Chimayo.Ssis.Writer2016.Executables.TaskCommon

open Chimayo.Ssis.Xml.Dsl
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi
open Chimayo.Ssis.Writer2016.Core
open Chimayo.Ssis.Writer2016.DtsIdMonad


let getBasicTaskElement typeName parents (tb:CftBase) =
    dtsIdState {
        let! refId, dtsId = RefIds.getExecutableIds parents tb.name

        let! vars = DefaultElements.buildVariables (tb.name :: parents) tb.variables
        let! los = DefaultElements.buildLoggingOptions tb.loggingOptions
        let es = DefaultElements.buildExpressions tb.expressions

        return 
            XmlElement.create "Executable" namespaceDts defaultNamespacesAndPrefixes
            |> XmlElement.setAttributes
                [
                    yield DefaultProperties.description tb.description
                    yield DefaultProperties.objectName tb.name
                    yield DefaultProperties.refId refId
                    yield DefaultProperties.dtsId dtsId
                    yield DefaultProperties.delayValidation tb.delayValidation
                    yield createDtsAttribute "ExecutableType" typeName
                    yield createDtsAttribute "CreationName" typeName
                    yield DefaultProperties.disabled tb.disabled
                    yield DefaultProperties.disableEventHandlers tb.disableEventHandlers
                    yield! DefaultProperties.forceExecutionResult tb.forcedExecutionResult
                    yield! DefaultProperties.forceExecutionValue tb.forcedExecutionValue
                    yield DefaultProperties.localeId tb.localeId
                    yield DefaultProperties.failParentOnFailure tb.failParentOnFailure
                    yield DefaultProperties.failPackageOnFailure tb.failPackageOnFailure
                    yield DefaultProperties.failOnErrorCountReaching tb.failOnErrorCountReaching
                    yield DefaultProperties.isolationLevel tb.isolationLevel
                    yield DefaultProperties.transactionOption tb.transactionOption
                ]
            |> XmlElement.setContent 
                [
                    yield vars
                    yield! es
                    yield los
                ]
               }



let buildPrecedenceConstraints parents pcsList =

    let buildConstraint (parents, name, mode, (pc : CfPrecedenceConstraint)) =
        dtsIdState {
            let! toRefId, toId = RefIds.getExecutableIds parents name
            let! fromRefId, fromId = RefIds.getExecutableIds parents pc.sourceExecutableName

            let pcQualifiedName = RefIds.getPrecedenceConstraintRefId parents pc.sourceExecutableName name
            let! pcName = 
              if System.String.IsNullOrWhiteSpace(pc.name) then
                DtsIdState.getDtsId pcQualifiedName |> DtsIdState.map (fun (g:System.Guid) -> g.ToString("P")) 
              else
                DtsIdState.value pc.name

            let op, expr, c = 
                match pc.logic with
                | CfPrecedenceConstraintLogic.Expression e -> CfPrecedenceConstraintMode.Expression, Some e, None
                | CfPrecedenceConstraintLogic.Constraint c -> CfPrecedenceConstraintMode.Constraint, None, Some c
                | CfPrecedenceConstraintLogic.ExpressionAndConstraint (e,c) -> CfPrecedenceConstraintMode.ExpressionAndConstraint, Some e, Some c
                | CfPrecedenceConstraintLogic.ExpressionOrConstraint (e,c) -> CfPrecedenceConstraintMode.ExpressionOrConstraint, Some e, Some c
            return
                createDtsElement "PrecedenceConstraint"
                |> XmlElement.setContent
                    [
                    ]
                |> XmlElement.setAttributes
                    [
                        yield createDtsAttribute "From" fromRefId
                        yield createDtsAttribute "To" toRefId
                        yield createDtsAttribute "LogicalAnd" mode
                        yield createDtsAttribute "EvalOp" (op |> (int) |> (string))
                        yield DefaultProperties.objectName pcName
                        yield DefaultProperties.description pc.description
                        yield! 
                            expr 
                            |> Option.toList
                            |> List.map (fun (CfExpression e) -> createDtsAttribute "Expression" e)
                        yield!
                            c
                            |> Option.toList
                            |> List.map ((int) >> (string))
                            |> List.map (createDtsAttribute "Value")
                            

                    ]
                   }

    let flatten (name : string, pc : CfPrecedenceConstraints) =
        let mode, cs =
            match pc with
            | CfPrecedenceConstraints.Empty -> true |> boolToTitleCase, []
            | CfPrecedenceConstraints.All cs -> true |> boolToTitleCase, cs
            | CfPrecedenceConstraints.Any cs -> false |> boolToTitleCase, cs
        
        cs |> List.map (fun c -> parents, name, mode, c)

    dtsIdState {
                    let constraints = 
                        pcsList 
                        |> List.map Executable.getConstraints
                        |> List.collect flatten
                    
                    let! results = constraints |> DtsIdState.listmap buildConstraint

                    return createDtsElement "PrecedenceConstraints"
                    |> XmlElement.setContent results
               }

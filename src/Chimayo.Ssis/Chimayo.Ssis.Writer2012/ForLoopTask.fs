module Chimayo.Ssis.Writer2012.Executables.ForLoopTask

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.Dsl
open Chimayo.Ssis.Writer2012.DtsIdMonad
open Chimayo.Ssis.Writer2012.Core
open Chimayo.Ssis.Writer2012.Executables.TaskCommon
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi

open Chimayo.Ssis.Writer2012

let getNewParents parents (t : CftForLoop) =
     t.executableTaskBase.name :: parents

let getExecutablesAndParentPath parents (t : CftForLoop) = 
    t.executables, getNewParents parents t

let build parents (t : CftForLoop) =
    dtsIdState {
        let parents' = t.executableTaskBase.name :: parents
        let! elem = getBasicTaskElement "STOCK:FORLOOP" parents t.executableTaskBase
        return
            elem
            |> XmlElement.addAttributes
                [
                    yield! 
                      t.initExpression
                      |> optionMapToList 
                           ( Expressions.getExpressionText >> createDtsAttribute @"InitExpression" )
                    yield
                      t.evalExpression
                      |> Expressions.getExpressionText
                      |> createDtsAttribute @"EvalExpression"
                    yield!
                      t.assignExpression
                      |> optionMapToList
                          ( Expressions.getExpressionText >> createDtsAttribute @"AssignExpression" )
                ]
            |> XmlElement.addContent
                [
                ]
               }



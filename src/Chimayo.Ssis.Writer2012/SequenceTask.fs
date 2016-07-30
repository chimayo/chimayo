module Chimayo.Ssis.Writer2012.Executables.SequenceTask

open Chimayo.Ssis.Xml.Dsl
open Chimayo.Ssis.Writer2012.DtsIdMonad
open Chimayo.Ssis.Writer2012.Core
open Chimayo.Ssis.Writer2012.Executables.TaskCommon
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi

open Chimayo.Ssis.Writer2012

let getNewParents parents (t : CftExecuteSequence) =
     t.executableTaskBase.name :: parents

let getExecutablesAndParentPath parents (t : CftExecuteSequence) = 
    t.executables, getNewParents parents t

let build parents (t : CftExecuteSequence) =
    dtsIdState {
        let! elem = getBasicTaskElement "STOCK:SEQUENCE" parents t.executableTaskBase

        return elem
               }

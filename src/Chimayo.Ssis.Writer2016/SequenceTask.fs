module Chimayo.Ssis.Writer2016.Executables.SequenceTask

open Chimayo.Ssis.Xml.Dsl
open Chimayo.Ssis.Writer2016.DtsIdMonad
open Chimayo.Ssis.Writer2016.Core
open Chimayo.Ssis.Writer2016.Executables.TaskCommon
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi

open Chimayo.Ssis.Writer2016

let getNewParents parents (t : CftExecuteSequence) =
     t.executableTaskBase.name :: parents

let getExecutablesAndParentPath parents (t : CftExecuteSequence) = 
    t.executables, getNewParents parents t

let build parents (t : CftExecuteSequence) =
    dtsIdState {
        let! elem = getBasicTaskElement "STOCK:SEQUENCE" parents t.executableTaskBase

        return elem
               }

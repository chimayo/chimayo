[<AutoOpen>]
module Chimayo.Ssis.Writer2016.Executables.TopLevel

open Chimayo.Ssis.Writer2016.Executables.TaskCommon

open Chimayo.Ssis.Xml.Dsl
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi
open Microsoft.FSharp.Collections

open Chimayo.Ssis.Writer2016
open Chimayo.Ssis.Writer2016.Core
open Chimayo.Ssis.Writer2016.DtsIdMonad

let rec createContainer fnCreate (exes, parents) =
    dtsIdState {
        let! elem = fnCreate
        let! exs = exes |> buildExecutables parents
        let! pcs = exes |> buildPrecedenceConstraints parents
        return elem |> XmlElement.addContent [ exs ; pcs ] 
               }



and buildExecutable parents t =
    match t with
    | CftExecuteSql t' -> ExecuteSqlTask.build parents t'
    | CftExecutePackageFromFile t' -> ExecutePackageTask.build parents t'
    | CftExecuteProcess t' -> ExecuteProcessTask.build parents t'
    | CftExpression t' -> ExpressionTask.build parents t'
    | CftSequence t' -> createContainer (SequenceTask.build  parents t') (SequenceTask.getExecutablesAndParentPath parents t')
    | CftForEachLoop t' -> createContainer (ForEachLoopTask.build parents t') (ForEachLoopTask.getExecutablesAndParentPath parents t')
    | CftForLoop t' -> createContainer (ForLoopTask.build parents t') (ForLoopTask.getExecutablesAndParentPath parents t')
    | CftPipeline t' -> PipelineTask.build parents t'

and buildExecutables parents exes =
    dtsIdState {
        let! content = exes |> List.sortBy Executable.getName |> DtsIdState.listmap (buildExecutable parents)

        let elem = 
            XmlElement.create "Executables" namespaceDts defaultNamespacesAndPrefixes
            |> XmlElement.setContent content

        return elem
               }






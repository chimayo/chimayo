module Chimayo.Ssis.CodeGen.Executables

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi

open Chimayo.Ssis.CodeGen.Internals
open Chimayo.Ssis.CodeGen.CodeDsl
open Chimayo.Ssis.CodeGen.CodeDsl.Builder
open Chimayo.Ssis.CodeGen.Core

let rec buildContainer executables (decls, ct) =
    let execListName, execDeclarations = build executables
    let ct' = ct |> extendRecordExpressions [ "executables", execListName ]
    decls @ [execDeclarations] , ct'

and buildExecutable prefixNames counter (executable : CftExecutable) =
     let nameFn = prefixNames |> (sprintf "e%d'%s" (counter + 1)) @?@ (sprintf "e'%s")
     let name = executable |> Executable.getName |> toCodeNameWithReplacement '_' |> nameFn
     let fn name (decls,ct) = decls, functionApplication (codename1<CftExecutable> name) [ ct ]
     let decls, ct =
        match executable with
        | CftExecutePackageFromFile ept -> 
            fn "CftExecutePackageFromFile" (ExecutePackageTask.build ept)
        | CftExecuteSql est ->
            fn "CftExecuteSql" (ExecuteSqlTask.build est)
        | CftExpression ext ->
            fn "CftExpression" (ExecuteExpressionTask.build ext)
        | CftExecuteProcess exp ->
            fn "CftExecuteProcess" (ExecuteProcessTask.build exp)
        | CftForEachLoop felt ->
            fn "CftForEachLoop" (buildContainer felt.executables (ForEachLoopTask.build felt))
        | CftForLoop flt ->
            fn "CftForLoop" (buildContainer flt.executables (ForLoopTask.build flt))
        | CftSequence st ->
            fn "CftSequence" (buildContainer st.executables (SequenceTask.build st))
        | CftPipeline pt ->
            fn "CftPipeline" (PipelineTask.build pt)

     let ct' =
        pipeline
            [
                yield ct
                yield! ExecutableTaskCommon.buildPipelinedBaseProperties executable
            ]
     name, LetBinding (false, name, None, [], combineDeclarations decls ct', None)

and buildSome (executables : CftExecutable list) =
    let bindingName = "executables"
    let executableNames, executableDecls  = 
        
        let prefixNames =
            let allNames = executables |> List.map (Executable.getName >> (toCodeNameWithReplacement '_'))
            let totalCount, distinctCount = allNames |> List.length, allNames |> Set.ofList |> Set.count
            totalCount > distinctCount
        
        let exes = executables |> List.mapi (buildExecutable prefixNames)
        exes |> List.map (fst >> NamedValue) |> ListExpression, exes |> List.map snd
    
    let listBinding = LetBinding (false, bindingName, None, [], combineDeclarations executableDecls executableNames, None)

    NamedValue bindingName, listBinding

and build (executables : CftExecutable list) : CodeTree * CodeTree =
    match executables with
    | [] -> NamedValue "[]", EmptyCodeTree
    | _ -> buildSome executables

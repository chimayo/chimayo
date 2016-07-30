module Chimayo.Ssis.CodeGen.PackageConfigurations

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators
open Chimayo.Ssis.CodeGen.Internals
open Chimayo.Ssis.CodeGen.CodeDsl
open Chimayo.Ssis.CodeGen.CodeDsl.Builder
open Chimayo.Ssis.CodeGen.Core

open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi


let buildParentVariableConfig (cfg : CfParentVariablePackageConfiguration) =

    let targetVariable, isVariableTarget =
        let x = System.Text.RegularExpressions.Regex.Match(cfg.targetProperty, @"^\\Package\.Variables\[([^]]+)].Properties\[Value\]$")
        if x.Success then x.Groups.[1].Value, true else "", false
    match isVariableTarget, cfg with
    | true, { description = "" ; expressions = [] } when cfg.parentVariableName = Variables.link targetVariable -> 
        let baseline = 
            PackageConfigurations.createSimpleParentVariableConfigurationWithName cfg.name (cfg.parentVariableName |> CfVariableRef.toString) cfg.ignoreParentVariableNamespace
            |> function CfParentVariablePackageConfiguration x -> x
        let fn field testVal expectedVal mapper = testVal = expectedVal |> [] @?@ [field, mapper expectedVal]
        functionApplicationQ <@ PackageConfigurations.createSimpleParentVariableConfigurationWithName @> 
            [ 
                cfg.name |> constant 
                cfg.parentVariableName |> CfVariableRef.toString |> constant
                cfg.ignoreParentVariableNamespace |> constant
            ]
        , false
    | _ -> 
        RecordExpression
            [
                "name", cfg.name |> constant
                "description", cfg.description |> constant
                "parentVariableName", cfg.parentVariableName |> makeScopedVariableReference
                "ignoreParentVariableNamespace", cfg.ignoreParentVariableNamespace |> constant
                "targetProperty", cfg.targetProperty |> constant
                "expressions", cfg.expressions |> buildPropertyExpressions
            ]
        , true
    

let buildConfig (cfg : CfPackageConfiguration) =
    match cfg with
    | CfParentVariablePackageConfiguration pvcfg -> 
        let ct, applyConstructor = buildParentVariableConfig pvcfg
        match applyConstructor with 
        | false -> ct 
        | _ -> functionApplication (codename1<CfPackageConfiguration> "CfParentVariablePackageConfiguration") [ct]

let build cfgs =
    let bindingName = "configs"
    let cfgs' = cfgs |> List.map buildConfig |> ListExpression
    NamedValue bindingName, LetBinding (false, bindingName, None, [], cfgs', None)
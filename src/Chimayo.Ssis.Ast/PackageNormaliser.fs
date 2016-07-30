/// Transformation of a package into a normalised format that can be used as the basis of comparison
/// or determinism in other activities. Functional characteristics and meaningful meta-data are retained.
module Chimayo.Ssis.Ast.ControlFlowApi.PackageNormaliser

open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi
open Chimayo.Ssis.Ast.DataFlow

let private normaliseVariables = List.sortBy Variables.getQualifiedName
let private normaliseExpressions = List.sortBy Expressions.getTargetProperty

let private normaliseLoggingOptions (lo : CfLoggingOptions) =
    // normalising log selections is not really safe; SSIS is extremely weird in the way it handles logging configuration
    // therefore, this function limits changes to the bare essentials
    
    let fn ls = { ls with columns = ls.columns |> List.sort } : CfLogEventSettings
    { lo with 
         logSelections = lo.logSelections |> List.map fn |> List.sortBy (fun lo -> lo.eventName) 
         logProviders = lo.logProviders |> List.sort
    }

let private normalisePrecedenceConstraints pc =
    let fn (pc : CfPrecedenceConstraint) = pc.sourceExecutableName
    match pc with
    | CfPrecedenceConstraints.Empty -> pc
    | CfPrecedenceConstraints.All pcs -> pcs |> List.sortBy fn |> CfPrecedenceConstraints.All
    | CfPrecedenceConstraints.Any pcs -> pcs |> List.sortBy fn |> CfPrecedenceConstraints.Any

let private normalisePipelineComponentData d =
    match d with
    | DfFlatFileSource d' ->
        DfFlatFileSource 
            { d' with fileColumns = d'.fileColumns |> List.sortBy (fun c -> c.name) }
    | DfDataConversion d' ->
        DfDataConversion
            { d' with columns = d'.columns |> List.sortBy (fun c -> c.name) }
    | DfDerivedColumn d' -> 
        DfDerivedColumn 
            { d' with columns = 
                        d'.columns 
                        |> List.sort 
                        |> List.map 
                            (function 
                                | { behaviour = DfDerivedColumnColumnBehaviour.NewColumn (n,d,e) } as column -> 
                                        { column with behaviour = DfDerivedColumnColumnBehaviour.NewColumn (n,d,e |> DfExpression.normalise) }
                                | { behaviour = DfDerivedColumnColumnBehaviour.ReplaceColumn (sc,e) } as column -> 
                                        { column with behaviour = DfDerivedColumnColumnBehaviour.ReplaceColumn (sc,e |> DfExpression.normalise) }
                            )
            }
    | DfLookup d' ->
        DfLookup
            { d' with 
                    joinColumns = d'.joinColumns |> List.sortBy (fun c -> c.referenceTableColumnName)
                    outputColumns = d'.outputColumns |> List.sortBy (fun c -> c.name)
            }
    | DfMulticast -> d
    | DfAggregate d' ->
        DfAggregate
            { d' with 
                    aggregations = 
                        d'.aggregations 
                        |> List.map (fun a -> { a with columns = a.columns |> List.sortBy (fun c -> c.name )})
                        |> List.sortBy (fun a -> a.outputName)
            }
    | DfConditionalSplit d' ->
        DfConditionalSplit 
            { d' with conditionalOutputs = 
                        d'.conditionalOutputs 
                        |> List.sortBy (fun o -> o.outputName) 
                        |> List.map (fun o -> { o with condition = o.condition |> DfExpression.normalise})
            }
    | DfUnionAll d' ->
        DfUnionAll
            { d' with columns = d'.columns |> List.sortBy (fun c -> c.name) |> List.map (fun c -> { c with mappedInputColumns = c.mappedInputColumns |> List.sort }) }
    | DfOleDbDestination d' ->
        DfOleDbDestination
            { d' with columns = d'.columns |> List.sortBy (fun c -> c.externalName) }
    | DfOleDbSource d' ->
        DfOleDbSource
            { d' with columns = d'.columns |> List.sortBy (fun c -> c.externalName) }
    | DfRecordsetDestination d' -> d // retain column order
    | DfRowCount _ -> d
    | DfXmlSource d' ->
        DfXmlSource
            { d' with outputs = 
                        d'.outputs
                        |> List.sortBy (fun o -> o.rowset)
                        |> List.map (fun o -> { o with columns = o.columns |> List.sortBy (fun c -> c.name )})
            }

let private normalisePipelineComponent c =
    {
        c with
            inputs = c.inputs |> List.sortBy DfNamedEntity.decode
            outputs = c.outputs  |> List.sortBy DfNamedEntity.decode
            outputColumns = c.outputColumns |> List.sortBy (fun ((DfName a),(DfName b),_) -> (a,b))
            inputConnections = c.inputConnections |> List.sortBy (DfInputConnection.decode >> fun (a,_,_) -> a)
            configuration = c.configuration |> normalisePipelineComponentData
    }

let private normalisePipeline p =
    {
        p with
            model =
                    {   p.model
                        with
                            components = p.model.components |> List.sortBy (fun c -> c.name) |> List.map normalisePipelineComponent
                    }
    }


let private normaliseTaskBase tb =
    {
        tb with
            expressions = tb.expressions |> normaliseExpressions
            variables = tb.variables |> normaliseVariables
            loggingOptions = tb.loggingOptions |> normaliseLoggingOptions
            precedenceConstraints = tb.precedenceConstraints |> normalisePrecedenceConstraints
    } : CftBase

let private normaliseExecutables exes =
    let normaliser exe = 
        let exe' = exe |> Executable.updateTaskBase normaliseTaskBase
        match exe' with
        | CftExecuteSql t ->
            CftExecuteSql
                { 
                    t with 
                        parameterBindings = t.parameterBindings |> List.sortBy (fun x -> x.parameterName)
                        resultBindings = t.resultBindings |> List.sortBy fst
                }
        | CftPipeline p ->
            p |> normalisePipeline |> CftPipeline
        | _ -> exe'
    exes |> Executable.map normaliser (List.sortBy Executable.getName)

/// Transform a package into a normalised format that can be used as the basis of comparison
/// or determinism in other activities. Functional characteristics and meaningful meta-data are retained.
let normalise (pkg : CftPackage) : CftPackage =
    {
        pkg with
            connectionManagers = pkg.connectionManagers |> List.sortBy ConnectionManager.getName
            configurations = pkg.configurations |> List.sortBy PackageConfigurations.getName
            logProviders = pkg.logProviders |> List.sortBy LogProviders.getName
            variables = pkg.variables |> normaliseVariables
            loggingOptions = pkg.loggingOptions |> normaliseLoggingOptions
            propertyExpressions = pkg.propertyExpressions |> normaliseExpressions
            executables = pkg.executables |> normaliseExecutables
    }
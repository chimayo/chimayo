/// Provides facilities to deeply transform a package
module Chimayo.Ssis.Ast.Transformation

open Chimayo.Ssis.Common
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi
open Chimayo.Ssis.Ast.DataFlow

/// <summary>Encoding of the path of an element within a package.
/// <para>Paths are supplied in reverse order with the most deeply nested name at the head of the list.</para>
/// </summary>
type AstPath = 
  /// <summary>Encoding of the path of an element within a package.
  /// <para>Paths are supplied in reverse order with the most deeply nested name at the head of the list.</para>
  /// </summary>
  | AstPath of string list 

/// A control flow executable type
module _AstExecutableType =
    /// A control flow executable type
    type T =
        | Sequence
        | ForEachLoop
        | ForLoop
        | ExecuteProcess
        | ExecutePackageFromFile
        | ExecuteSql
        | Expression
        | Pipeline
        /// Convert an executable definition to its corresponding AstExecutableType case
        static member asAst = function
            | CftSequence _ -> Sequence
            | CftForEachLoop _ -> ForEachLoop
            | CftForLoop _ -> ForLoop
            | CftExecuteProcess _ -> ExecuteProcess
            | CftExecutePackageFromFile _ -> ExecutePackageFromFile
            | CftExecuteSql _ -> ExecuteSql
            | CftExpression _ -> Expression
            | CftPipeline _ -> Pipeline

/// A control flow executable type
type AstExecutableType = _AstExecutableType.T

/// A data flow component type
module _AstPipelineComponentType = 
    /// A data flow component type
    type T =
        | OleDbSource
        | FlatFileSource
        | XmlSource
        | DataConversion
        | DerivedColumn
        | Lookup
        | Aggregate
        | ConditionalSplit
        | UnionAll
        | Multicast
        | RowCount
        | OleDbDestination
        | RecordsetDestination
        /// Convert a component definition to its corresponding AstPipelineComponentType case
        static member asAst c = 
                            match c.configuration with
                            | DfOleDbSource _ -> OleDbSource
                            | DfFlatFileSource _ -> FlatFileSource
                            | DfXmlSource _ -> XmlSource
                            | DfDataConversion _ -> DataConversion
                            | DfDerivedColumn _ -> DerivedColumn
                            | DfLookup _ -> Lookup
                            | DfAggregate _ -> Aggregate
                            | DfConditionalSplit _ -> ConditionalSplit
                            | DfUnionAll _ -> UnionAll
                            | DfMulticast _ -> Multicast
                            | DfOleDbDestination _ -> OleDbDestination
                            | DfRecordsetDestination _ -> RecordsetDestination
                            | DfRowCount _ -> RowCount

/// A data flow component type
type AstPipelineComponentType = _AstPipelineComponentType.T

/// A package configuration type
module _AstPackageConfigurationType =
    /// A package configuration type
    type T =
        | ParentVariable
        /// Convert a package configuration definition to its corresponding AstPackageConfigurationType case
        static member asAst = function | CfParentVariablePackageConfiguration _ -> ParentVariable

/// A package configuration type
type AstPackageConfigurationType = _AstPackageConfigurationType.T

/// A connection manager type
module _AstConnectionManagerType =
    /// A connection manager type
    type T =
        | AdoNet
        | File
        | FlatFile
        | OleDb
        /// Convert a connection manager definition to its corresponding AstConnectionManagerType case
        static member asAst = 
                        function
                        | CfAdoNetSqlConnectionManager _ -> AdoNet
                        | CfFlatFileConnectionManager _ -> FlatFile
                        | CfFileConnectionManager _ -> File
                        | CfOleDbConnectionManager _ -> OleDb

/// A connection manager type
type AstConnectionManagerType = _AstConnectionManagerType.T

/// All parts of a package are represented as cases within the AstElement discriminated union
type AstElement =
| AstPackage of CftPackage
| AstPackageConfiguration of string * AstPackageConfigurationType * CfPackageConfiguration
| AstConnectionManager of string * AstConnectionManagerType * CfConnectionManager
| AstVariable of AstPath * CfVariable
| AstExecutable of AstPath * AstExecutableType * CftExecutable
| AstPipelineComponent of AstPath * AstPipelineComponentType * DfComponent

/// Represents a transformed or replaced version of an AstElement
module _AstElementResult =
    /// Represents a transformed or replaced version of an AstElement
    type T =
        | Package of CftPackage
        | PackageConfiguration of CfPackageConfiguration
        | ConnectionManager of CfConnectionManager
        | Variable of CfVariable
        | Executable of CftExecutable
        | PipelineComponent of DfComponent
        /// Translates an AstElement to the corresponding AstElementResult value
        static member ofAst =
            function
            | AstElement.AstPackage p -> Package p
            | AstElement.AstPackageConfiguration (_,_,c) -> PackageConfiguration c
            | AstElement.AstConnectionManager (_,_,c) -> ConnectionManager c
            | AstElement.AstVariable (_,v) -> Variable v
            | AstElement.AstExecutable (_,_,e) -> Executable e
            | AstElement.AstPipelineComponent (_,_,c) -> PipelineComponent c

/// Represents a transformed or replaced version of an AstElement
type AstElementResult = _AstElementResult.T

/// Internal functions to support transformation of packages
module private Internals =
    let asPackage = function AstElementResult.Package p -> p | _ -> failwith "invalid"
    let asVariable = function AstElementResult.Variable v -> v | _ -> failwith "invalid"
    let asPackageConfiguration = function AstElementResult.PackageConfiguration c -> c | _ -> failwith "invalid"
    let asConnectionManager = function AstElementResult.ConnectionManager c -> c | _ -> failwith "invalid"
    let asExecutable = function AstElementResult.Executable e -> e | _ -> failwith "invalid"
    let asComponent = function AstElementResult.PipelineComponent c -> c | _ -> failwith "invalid"


    let flattenConfiguration cfg = AstPackageConfiguration ( cfg |> PackageConfigurations.getName, cfg |> AstPackageConfigurationType.asAst, cfg )
    let flattenConnectionManager conn = AstConnectionManager (conn |> ConnectionManager.getName, conn |> AstConnectionManagerType.asAst, conn)
    let flattenVariable path v =
        AstVariable
            ( AstPath ( (v |> Variables.getQualifiedName |> uncurry (sprintf "%s::%s")) :: path )
            , v 
            )
    let flattenExecutable completePath e = AstExecutable (completePath |> AstPath, e |> AstExecutableType.asAst, e)
    let flattenComponent path c= AstPipelineComponent (c.name::path |> AstPath , c |> AstPipelineComponentType.asAst, c)

/// Provides facilities to deeply transform a package
module Transform =
    /// Flattens the object graph of a package into a list of AstElement values
    let flatten (pkg:CftPackage) =
    
        let flattenP path =
            function
            | CftPipeline p -> p.model.components |> List.map (Internals.flattenComponent path)
            | _ -> []

        let rec flattenE path e =
            let name = e |> Executable.getName
            let path' = name::path
            [
                yield Internals.flattenExecutable path' e
                yield! e |> Executable.getVariables |> List.map (Internals.flattenVariable path')
                yield! e |> Executable.getExecutables |> List.collect (flattenE path')
                yield! e |> flattenP path'
            ]

        [
            yield AstPackage pkg
            yield! pkg.configurations |> List.map Internals.flattenConfiguration
            yield! pkg.connectionManagers |> List.map Internals.flattenConnectionManager
            yield! pkg.variables |> List.map (Internals.flattenVariable [])
            yield! pkg.executables |> List.collect (flattenE [])
        ]

    /// <summary>Performs a deep transformation on a package.
    /// <para>The mapper can remove, replace or add elements to the package. Removing elements
    /// is achieved by returning a value of None.  Values should always be returned of the same
    /// element type as the type matched.</para>
    /// <para>The mapping is performed hierarchically, with parents being transformed before
    /// children.  This allows new children to be injected and subsequently transfomed.</para>
    /// <para>It is possible to chain transformations as often as desired, however the translation
    /// is most efficient when transformations are combined into a single mapper function.</para>
    /// </summary>
    let map mapper pkg =
        let transformV path = Internals.flattenVariable path >> mapper >> Option.map Internals.asVariable

        let transformP path e =
            match e with
            | CftPipeline _ -> 
                e
                |> Pipeline.clearComponents
                |> Pipeline.addComponents 
                    ( e |> Pipeline.getComponents |> List.choose (Internals.flattenComponent path >> mapper >> Option.map Internals.asComponent) )
            | _ -> e

        let rec transformE path e : CftExecutable option =
            let name = e |> Executable.getName
            let path' = name::path
            let e' = 
                e 
                |> Internals.flattenExecutable path' 
                |> mapper 
                |> Option.map Internals.asExecutable
                |> Option.map (transformP path')

            e'
            |> Option.map
                (fun e'' ->
                    e''
                    |> Executable.clearVariables
                    |> Executable.addVariables (e'' |> Executable.getVariables |> List.choose (transformV path'))
                    |> Executable.clearExecutables
                    |> Executable.addExecutables (e'' |> Executable.getExecutables |> List.choose (transformE path'))
                )
    
        let pkg' = AstPackage pkg |> mapper |> Option.get |> Internals.asPackage
        {
            pkg'
            with
                configurations = pkg'.configurations |> List.choose (Internals.flattenConfiguration >> mapper >> Option.map Internals.asPackageConfiguration)
                connectionManagers = pkg'.connectionManagers |> List.choose (Internals.flattenConnectionManager >> mapper >> Option.map Internals.asConnectionManager)
                variables = pkg'.variables |> List.choose (transformV [])
                executables = pkg'.executables |> List.choose (transformE [])
        }

    /// Get an executable by path from a flattened package
    let getExecutableByPath path = List.choose (function | AstExecutable (AstPath path',_,e) when path = path' -> Some e | _ -> None) >> List.head
    /// Get a pipeline component by path from a flattened package
    let getPipelineComponentByPath path = List.choose (function | AstPipelineComponent (AstPath path',_,c) when path = path' -> Some c | _ -> None) >> List.head
    /// Get a connection manager by name from a flattened package
    let getConnectionManagerByName name = List.choose (function | AstConnectionManager (name',_,cm) when stringCompareInvariant name name' -> Some cm | _ -> None) >> List.head
    /// Get a variable by path from a flattened package
    let getVariableByPath path = List.choose (function | AstVariable (AstPath path',v) when path = path' -> Some v | _ -> None) >> List.head
    
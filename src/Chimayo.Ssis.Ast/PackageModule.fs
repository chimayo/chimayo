module Chimayo.Ssis.Ast.ControlFlowApi.Package

open System
open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Ast
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi

/// Default empty package
let empty = CftPackage.Empty

/// Create a new package with the provided name
let create name = { empty with name = name }

/// Get the name of a package
let getName (pkg:CftPackage) = pkg.name
/// Set the name of a package
let setName name (pkg:CftPackage) = { pkg with name = name }

/// Get the creation date for a package
let getCreationDate (pkg:CftPackage) = pkg.creationDate
/// Set the creation date for a package
let setCreationDate creationDate (pkg:CftPackage) = { pkg with creationDate = creationDate }

// ------------------ Connection Managers

/// Get the package connection managers
let getConnectionManagers pkg = pkg.connectionManagers

/// Add a connection manager to a package
let addConnectionManager cm pkg = 
    let adder cm pkg = { pkg with connectionManagers = cm :: pkg.connectionManagers }
    let tryAdd' = tryAdd getConnectionManagers ConnectionManager.getName adder stringCompareInvariantIgnoreCase
    pkg |> tryAdd' cm

/// Add multiple connection managers to a package
let addConnectionManagers = swapAndFoldList addConnectionManager

/// Remove all connection managers from a package
let clearConnectionManagers pkg = { pkg with connectionManagers = [] }

// ------------------ Executables

/// Get the top level executables in a package
let getExecutables (pkg : CftPackage) = pkg.executables

/// Add an executable to a package
let addExecutable e (pkg : CftPackage) =
    let adder e p : CftPackage = { p with executables = e :: p.executables } 
    let tryAdd' = tryAdd getExecutables Executable.getName adder stringCompareInvariantIgnoreCase
    pkg |> tryAdd' e

/// Add multiple executables to a package
let addExecutables = swapAndFoldList addExecutable

/// Remove all executables from a package
let clearExecutables pkg : CftPackage = { pkg with executables = [] }

// ------------------ Basic properties

/// Get the disable flag on a package
let getDisabled (pkg:CftPackage) = pkg.disabled
/// Set the disable flag on a package
let setDisabled disabled pkg : CftPackage = { pkg with disabled = disabled }
/// Enable a package
let enable = setDisabled false
/// Disable a package
let disable = setDisabled true

/// Get the disable event handlers flag on a package
let getDisableEventHandlers (pkg:CftPackage) = pkg.disableEventHandlers
/// Set the disable event handlers flag on a package
let setDisableEventHandlers enabled pkg : CftPackage = { pkg with disableEventHandlers = enabled }
/// Disable event handlers on a package
let disableEventHandlers = setDisableEventHandlers true
/// Enable event handlers on a package
let enableEventHandlers = setDisableEventHandlers false

/// Get the value of the delayValidation flag on a package
let getDelayValidation (pkg:CftPackage) = pkg.delayValidation
/// Set the value of the delayValidation flag on a package
let setDelayValidation delayValidation (pkg:CftPackage) = { pkg with delayValidation = delayValidation }
/// Set the value of the delayValidation flag on a package
let delayValidation = setDelayValidation

/// Get the forced execution result value on a package
let getForcedExecutionResult (pkg:CftPackage) = pkg.forcedExecutionResult
/// Set the forced execution result value on a package
let setForcedExecutionResult resultOption pkg = { pkg with forcedExecutionResult = resultOption } : CftPackage

/// Clear the forced execution result on a package
let clearForcedExecutionResult = setForcedExecutionResult None
/// Set a forced execution result on a package
let forceExecutionResult result = setForcedExecutionResult (Some result)
/// Set the forced execution result on a package to Success
let forceSuccess = forceExecutionResult CfExecutableResult.Success
/// Set the forced execution result on a package to Failure
let forceFailure = forceExecutionResult CfExecutableResult.Failure
/// Set the forced execution result on a package to Completion
let forceCompletion = forceExecutionResult CfExecutableResult.Completion

/// Get the forced execution value on a package
let getForcedResultValue (pkg : CftPackage) = pkg.forcedExecutionValue
/// Set the forced execution value on a package
let setForcedResultValue valueOption (pkg:CftPackage) = { pkg with forcedExecutionValue = valueOption }
/// Clear the forced execution value on a package
let clearForcedResultValue = setForcedResultValue None
/// Set a forced execution value on a package with a supplied CfData value
let forceResultValueDirect value = value |> Some |> setForcedResultValue
/// Set a forced execution value on a package, constructed via CfData.create
let forceResultValue value = value |> CfData.create |> forceResultValueDirect

/// Get the fail parent on failure flag on a package
let getFailParentOnFailure (pkg:CftPackage) = pkg.failParentOnFailure
/// Set the fail parent on failure flag on a package
let setFailParentOnFailure failParentOnFailure (pkg:CftPackage) = { pkg with failParentOnFailure = failParentOnFailure }
/// Set the fail parent on failure flag on a package
let failParentOnFailure = setFailParentOnFailure

/// Get the fail parent on error count reaching value on a package
let getFailOnErrorCountReaching (pkg:CftPackage) = pkg.failOnErrorCountReaching
/// Set the fail parent on error count reaching value on a package
let setFailOnErrorCountReaching failOnErrorCountReaching (pkg:CftPackage) = { pkg with failOnErrorCountReaching = failOnErrorCountReaching } : CftPackage
/// Set the fail parent on error count reaching value on a package
let failOnErrorCountReaching = setFailOnErrorCountReaching

/// Get the max concurrent executables value on a package
let getMaxConcurrentExecutables (pkg:CftPackage) = pkg.maxConcurrentExecutables
/// Set the max concurrent executables value on a package
let setMaxConcurrentExecutables maxConcurrentExecutables (pkg:CftPackage) = { pkg with maxConcurrentExecutables = maxConcurrentExecutables }
/// Set the max concurrent executables value on a package
let maxConcurrentExecutables = setMaxConcurrentExecutables

/// Get the isolation level on a package
let getIsolationLevel (pkg: CftPackage) = pkg.isolationLevel
/// Set the isolation level on a package
let setIsolationLevel isolationLevel pkg = { pkg with isolationLevel = isolationLevel } : CftPackage
/// Set the isolation level on a package
let isolationLevel = setIsolationLevel

/// Get the transaction option on a package
let getTransactionOption (pkg: CftPackage) = pkg.transactionOption
/// Set the transaction option on a package
let setTransactionOption transactionOption pkg = { pkg with transactionOption = transactionOption } : CftPackage
/// Set the transaction option on a package
let transactionOption = setTransactionOption

/// Get the locale identifier on a package
let getLocaleId (pkg : CftPackage) = pkg.localeId
/// Set the locale identifier on a package
let setLocaleId localeId (pkg : CftPackage) = { pkg with localeId = localeId }
/// Set the locale identifier on a package
let localeId = setLocaleId

// ------------------ Variables

/// Get package variables
let getVariables (pkg : CftPackage) = pkg.variables

/// Add a variable to a package
let addVariable var pkg =
    let adder var pkg = { pkg with variables = var :: pkg.variables } : CftPackage
    let comparer (a,b) (c,d) = stringCompareInvariantIgnoreCase a c && stringCompareInvariantIgnoreCase b d
    let tryAdd' = tryAdd getVariables Variables.getQualifiedName adder comparer
    pkg |> tryAdd' var

/// Add multiple variables to a package
let addVariables = swapAndFoldList addVariable 

/// Clear package variables
let clearVariables (pkg : CftPackage) = { pkg with variables = [] }

// ------------------ Parameters

/// Get package parameters
let getParameters (pkg : CftPackage) = pkg.parameters

/// Add a parameter to a package
let addParameter par pkg =
    let adder par pkg = { pkg with parameters = par :: pkg.parameters } : CftPackage
    let comparer (a,b) (c,d) = stringCompareInvariantIgnoreCase a c && stringCompareInvariantIgnoreCase b d
    let tryAdd' = tryAdd getParameters Parameters.getQualifiedName adder comparer
    pkg |> tryAdd' par

/// Add multiple parameters to a package
let addParameters = swapAndFoldList addParameter 

/// Clear package parameters
let clearParameters (pkg : CftPackage) = { pkg with parameters = [] }


// ------------------ Logging

/// Get the logging options defined on a package
let getLogging (pkg:CftPackage) = pkg.loggingOptions
/// Set the logging options for apackage
let setLogging loggingOptions pkg = { pkg with loggingOptions = loggingOptions } : CftPackage

/// Disable logging on a package
let disableLogging pkg =
    pkg |> setLogging ( LoggingOptions.create CfLogMode.Disabled CfLogFilterKind.ByExclusion [] )

/// <summary>Enable explicit logging on a package.  Sets the ByInclusion mode so log selections should be added or this should be adapted.
/// <para>Customised behaviour can be achieved by using setLogging directly.</para>
/// <para>Note that this clears existing log selections.</para></summary>
let enableLogging useParentSetting providers pkg =
    let options = 
        LoggingOptions.create 
            (useParentSetting |> CfLogMode.UseParentSetting @?@ CfLogMode.Enabled)
            CfLogFilterKind.ByInclusion
            providers
    pkg |> setLogging options

/// Apply log selections to the current logging configuration.
let configureLogging logSelections pkg =
    { pkg with loggingOptions = pkg.loggingOptions |> LoggingOptions.configureLogging logSelections } : CftPackage

// ------------------ Package configurations

/// Get the value of the enable configurations flag on a package
let getEnableConfigurations (pkg:CftPackage) = pkg.enableConfigurations
/// Set the value of the enable configurations flag on a package
let setEnableConfigurations enableConfigurations pkg = { pkg with enableConfigurations = enableConfigurations }
/// Enable package configurations
let enableConfigurations = setEnableConfigurations true
/// Disable package configurations
let disableConfigurations = setEnableConfigurations false

/// Get package configurations
let getConfigurations (pkg : CftPackage) = pkg.configurations

/// Add a package configuration
let addConfiguration cfg pkg =
    let adder cfg pkg = { pkg with configurations = cfg :: pkg.configurations }
    let tryAdd' = tryAdd getConfigurations PackageConfigurations.getName adder stringCompareInvariantIgnoreCase
    pkg |> tryAdd' cfg

/// Add multiple package configurations
let addConfigurations = swapAndFoldList addConfiguration

/// Remove all package configurations
let clearConfigurations (pkg:CftPackage) = { pkg with configurations = [] }

// ------------------ Property Expressions

/// Get property expressions
let getExpressions (pkg : CftPackage) = pkg.propertyExpressions
/// Add a property expression
let addExpression e pkg =
    let adder e pkg = { pkg with propertyExpressions = e :: pkg.propertyExpressions }
    let tryAdd' = tryAdd getExpressions Expressions.getTargetProperty adder stringCompareInvariantIgnoreCase
    pkg |> tryAdd' e
/// Add multiple property expressions
let addExpressions = swapAndFoldList addExpression
/// Remove all property expressions
let clearExpressions (pkg : CftPackage) = { pkg with propertyExpressions = [] }


// ------------------ Log providers

/// Get log providers
let getLogProviders (pkg:CftPackage) = pkg.logProviders
/// Add a log provider
let addLogProvider lp (pkg:CftPackage) =
    let adder lp pkg = { pkg with logProviders = lp :: pkg.logProviders } : CftPackage
    let tryAdd' = tryAdd getLogProviders LogProviders.getName adder stringCompareInvariantIgnoreCase
    pkg |> tryAdd' lp

/// Add multiple log providers
let addLogProviders = swapAndFoldList addLogProvider
/// Remove all log providers
let clearLogProviders (pkg:CftPackage) = { pkg with logProviders = [] }

/// API for working with log providers on a package
[<Obsolete("Use the functions exposed on the parent module")>]
module LogProviders =
    /// Get log providers
    let getLogProviders = getLogProviders
    /// Add a log provider
    let addLogProvider = addLogProvider
    /// Add multiple log providers
    let addLogProviders = addLogProviders
    /// Remove all log providers
    let clearLogProviders = clearLogProviders




namespace Chimayo.Ssis.Ast.ControlFlow

open Chimayo.Ssis.Ast
open Chimayo.Ssis.Ast.DataFlow

/// Common properties for all executable tasks
type CftBase = 
    {
        description: string
        name: string
        expressions: CfPropertyExpressions

        variables: CfVariable list
        loggingOptions: CfLoggingOptions
        precedenceConstraints: CfPrecedenceConstraints

        forcedExecutionValue: CfData option
        forcedExecutionResult: CfExecutableResult option
        disabled: bool
        failPackageOnFailure: bool
        failParentOnFailure: bool
        failOnErrorCountReaching: int
        isolationLevel: CfIsolationLevel
        localeId: int
        transactionOption: CfTransactionOption
        delayValidation: bool
        disableEventHandlers: bool
    }

/// Source data that can be supplied directly as a string or indirectly via another package element, typically a variable or file connection
type CfIndirectSource =
    | CfDirectSource of string
    | CfIndirectSource of CfRef

/// Parameter binding definition for the Execute Sql task
type CfExecuteSqlParameterBinding =
    {
        parameterName: string
        targetVariable: CfVariableRef
        direction: CfParameterDirection
        dataType : CfDataType
        parameterSize : int option
    }

/// Execute Sql task
type CftExecuteSql =
    {
        executableTaskBase: CftBase

        connection: CfRef
        timeoutSeconds: int
        isStoredProc: bool
        bypassPrepare: bool

        source: CfIndirectSource
        codePage: int
        resultType: CfExecuteSqlResult

        parameterBindings: CfExecuteSqlParameterBinding list
        resultBindings: (string * CfVariableRef) list
    }

/// Execute Package task
type CftExecutePackageFromFile =
    {
        executableTaskBase: CftBase
    
        executeOutOfProcess: bool
        connection: CfRef
    }

/// Expression task
type CftExpression =
    {
        executableTaskBase: CftBase

        expression : CfExpression
    }

/// Initial window style for processes launched via the Execute Process task
module _CfWindowStyle = 
  /// Initial window style for processes launched via the Execute Process task
    type T =
        | Hidden
        | Maximized
        | Minimized
        | Normal

/// Initial window style for processes launched via the Execute Process task
type CfWindowStyle = _CfWindowStyle.T

/// Execute Process task
type CftExecuteProcess =
    {
        executableTaskBase: CftBase

        targetExecutable: string
        requireFullFilename: bool
        arguments: string list  
        workingDirectory: string
        failTaskOnReturnCodeNotEqualToValue: int option
        terminateAfterTimeoutSeconds: int option
        standardInputVariable: CfVariableRef option
        standardOutputVariable: CfVariableRef option
        standardErrorVariable: CfVariableRef option
        windowStyle: CfWindowStyle
    }

/// Pipeline task
type CftPipeline =
    {
        executableTaskBase : CftBase

        defaultBufferMaxRows : int option
        engineThreadsHint : int option
        defaultBufferSize : int option
        blobTempStoragePath : string option
        bufferTempStoragePath : string option
        runInOptimizedMode : bool option

        model : DfPipeline
    }

/// File name style for the For Each Loop task when configured to use file enumeration
type CfForEachLoopFileNameRetrievalFormat =
    | FullyQualifiedFileName = 0
    | FileNameAndExtension = 1
    | FileNameOnly = 2

/// File enumeration configuration for the For Each Loop task
type CfForEachLoopFileEnumeratorConfiguration =
    {
        folderPath : string
        fileSpec : string
        fileNameRetrievalFormat : CfForEachLoopFileNameRetrievalFormat
        recurse: bool
    }

/// Data set enumeration configuration for the For Each Loop task
type CfForEachLoopDataSetMode =
    | CfEnumerateRowsInFirstTable
    | CfEnumerateAllRows
    | CfEnumerateTables
    /// Interprets the internal SSIS textual value to a discriminated union case
    static member fromString = 
                   function
                   | "EnumerateRowsInFirstTable" -> CfEnumerateRowsInFirstTable
                   | "EnumerateAllRows" -> CfEnumerateAllRows
                   | "EnumerateTables" -> CfEnumerateTables
                   | _ -> failwith "Unsupported case"
    /// Converts the discriminated union case to the internal SSIS textual value
    static member toString : CfForEachLoopDataSetMode -> string = 
                   function
                   | CfEnumerateRowsInFirstTable -> "EnumerateRowsInFirstTable"
                   | CfEnumerateAllRows -> "EnumerateAllRows"
                   | CfEnumerateTables -> "EnumerateTables"


/// Behaviour of the outer For Each Loop node list iterator
module _CfForEachLoopNodeListMode =
    /// Behaviour of the outer For Each Loop node list iterator
    type T =
        | Navigator
        | Node
        | NodeText
        | ElementCollection
        /// Interprets the internal SSIS textual value to a discriminated union case
        static member fromString = 
                       function
                       | "Navigator" -> Navigator
                       | "Node" -> Node
                       | "NodeText" -> NodeText
                       | "ElementCollection" -> ElementCollection
                       | _ -> failwith "Unsupported case"
        /// Converts the discriminated union case to the internal SSIS textual value
        static member toString : T -> string = 
                       function
                       | Navigator -> "Navigator"
                       | Node -> "Node"
                       | NodeText -> "NodeText"
                       | ElementCollection -> "ElementCollection"

/// Behaviour of the outer For Each Loop node list iterator
type CfForEachLoopNodeListMode = _CfForEachLoopNodeListMode.T

/// Behaviour of the inner For Each Loop node list iterator
module _CfForEachLoopNodeListInnerMode = 
    /// Behaviour of the inner For Each Loop node list iterator
    type T =
        | Navigator
        | Node
        | NodeText
        /// Interprets the internal SSIS textual value to a discriminated union case
        static member fromString = 
                       function
                       | "Navigator" -> Navigator
                       | "Node" -> Node
                       | "NodeText" -> NodeText
                       | _ -> failwith "Unsupported case"
        /// Converts the discriminated union case to the internal SSIS textual value
        static member toString : T -> string = 
                       function
                       | Navigator -> "Navigator"
                       | Node -> "Node"
                       | NodeText -> "NodeText"

/// Behaviour of the inner For Each Loop node list iterator
type CfForEachLoopNodeListInnerMode = _CfForEachLoopNodeListInnerMode.T

/// Configuration of the For Each Loop node list enumerator
type CfForEachLoopNodeListEnumeratorConfiguration =
    {
        logic : CfForEachLoopNodeListMode
        innerLogic : CfForEachLoopNodeListInnerMode
        outerXPathSource : CfIndirectSource
        innerXPathSource : CfIndirectSource
        source : CfIndirectSource
    }

/// Behaviour of the For Each Loop task enumerator
type CfForEachLoopLogic =
    | CfForEachLoopItemCollection of CfData list list
    | CfForEachLoopFileEnumerator of CfForEachLoopFileEnumeratorConfiguration
    | CfForEachLoopDataSet of CfForEachLoopDataSetMode * CfVariableRef
    | CfForEachLoopVariable of CfVariableRef
    | CfForEachLoopNodeList of CfForEachLoopNodeListEnumeratorConfiguration

/// Variable mappings for the For Each Loop task
type CfForEachLoopVariableMapping =
    {
        expressions : CfPropertyExpressions
        target : CfVariableRef
    }

/// Sequence task
type CftExecuteSequence =
    {
        executableTaskBase: CftBase

        executables: CftExecutable list
    }       

/// For Each Loop task
and CftForEachLoop =
    {
        executableTaskBase: CftBase

        executables: CftExecutable list
        enumerationExpressions : CfPropertyExpressions
        enumeratorLogic : CfForEachLoopLogic
        variableMappings : CfForEachLoopVariableMapping list
    }

and CftForLoop =
    {
      executableTaskBase: CftBase

      executables: CftExecutable list
      initExpression: CfExpression option
      evalExpression: CfExpression
      assignExpression: CfExpression option
    }

/// Discriminated union of all executable tasks
and CftExecutable =
    | CftExecuteSql of CftExecuteSql
    | CftExecutePackageFromFile of CftExecutePackageFromFile
    | CftExecuteProcess of CftExecuteProcess
    | CftExpression of CftExpression
    | CftSequence of CftExecuteSequence
    | CftForLoop of CftForLoop
    | CftForEachLoop of CftForEachLoop
    | CftPipeline of CftPipeline








namespace Chimayo.Ssis.Ast.ControlFlowApi

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Ast
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Ast.ControlFlowApi
open Chimayo.Ssis.Ast.DataFlowApi


module Executable =
    
    let getTaskBase = function
        | CftExecuteSql t -> t.executableTaskBase
        | CftExecutePackageFromFile t -> t.executableTaskBase
        | CftExpression t -> t.executableTaskBase
        | CftExecuteProcess t -> t.executableTaskBase
        | CftSequence t -> t.executableTaskBase
        | CftForEachLoop t -> t.executableTaskBase
        | CftForLoop t -> t.executableTaskBase
        | CftPipeline t -> t.executableTaskBase

    let setTaskBase newTb = function
        | CftExecuteSql t -> CftExecuteSql {t with executableTaskBase = newTb }
        | CftExecutePackageFromFile t -> CftExecutePackageFromFile {t with executableTaskBase = newTb }
        | CftExpression t -> CftExpression { t with executableTaskBase = newTb }
        | CftExecuteProcess t -> CftExecuteProcess { t with executableTaskBase = newTb }
        | CftSequence t -> CftSequence { t with executableTaskBase = newTb }
        | CftForEachLoop t -> CftForEachLoop { t with executableTaskBase = newTb }
        | CftForLoop t -> CftForLoop { t with executableTaskBase = newTb }
        | CftPipeline t -> CftPipeline { t with executableTaskBase = newTb }

    let createTaskBase name =
        {
            description= ""
            name= name
            expressions= []

            variables= []
            loggingOptions= LoggingOptions.defaults
            precedenceConstraints= CfPrecedenceConstraints.Empty

            forcedExecutionValue= None
            forcedExecutionResult= None
            disabled= false
            failPackageOnFailure= false
            failParentOnFailure= false
            failOnErrorCountReaching= 1
            isolationLevel= CfIsolationLevel.ReadCommitted
            localeId= System.Globalization.CultureInfo.InvariantCulture.LCID
            transactionOption= CfTransactionOption.Supported
            delayValidation= true
            disableEventHandlers = false
        }

    let updateTaskBase fn task =
        task |> setTaskBase (task |> getTaskBase |> fn)

    let getName ex = (getTaskBase ex).name
    let setName value = updateTaskBase <| fun tb -> { tb with name = value }

    let getConstraints ex =
        ex
        |> getTaskBase
        |> (fun tb -> tb.precedenceConstraints)
        |> (fun x -> ex |> getName, x)

    let clearPrecedenceConstraints = updateTaskBase <| fun tb -> { tb with precedenceConstraints = CfPrecedenceConstraints.Empty }
    let getPrecedenceConstraints = getTaskBase >> fun tb -> tb.precedenceConstraints
    let setPrecedenceConstraints pcs = updateTaskBase <| fun tb -> { tb with precedenceConstraints = pcs }

    let addPrecedenceConstraint requireAll precConstraint =
        let update taskBase =
            let fn = CfPrecedenceConstraints.addAll @?@ CfPrecedenceConstraints.addAny <| requireAll
            {
                taskBase
                with precedenceConstraints = taskBase.precedenceConstraints |> fn precConstraint
            }
        updateTaskBase update

    let configureLogging logSelections =
        updateTaskBase
        <| fun tb -> { tb with loggingOptions = tb.loggingOptions |> LoggingOptions.configureLogging logSelections }

    let getVariables task = task |> getTaskBase |> fun tb -> tb.variables
    let getExecutables = function CftSequence s -> s.executables | CftForEachLoop f -> f.executables | _ -> []

    let clearVariables = updateTaskBase <| fun tb -> { tb with variables = [] }

    let addVariable variable =
        let adder var = updateTaskBase <| fun tb -> { tb with variables = var :: tb.variables }
        let comparer (a,b) (c,d) = stringCompareInvariantIgnoreCase a c && stringCompareInvariantIgnoreCase b d
        let tryAdd' = tryAdd getVariables Variables.getQualifiedName adder comparer
        tryAdd' variable

    let addVariables = swapAndFoldList addVariable

    let clearExecutables task =
        match task with
        | CftExecuteSql _ -> task
        | CftExecutePackageFromFile _ -> task
        | CftExecuteProcess _ -> task
        | CftExpression _ -> task
        | CftPipeline _ -> task
        | CftForEachLoop f -> CftForEachLoop { f with executables = [] }
        | CftForLoop f -> CftForLoop { f with executables = [] }
        | CftSequence s -> CftSequence { s with executables = [] }

    let addExecutable executable =
        function
        | CftExecuteSql _ -> failwith "invalid task type"
        | CftExecutePackageFromFile _ -> failwith "invalid task type"
        | CftExecuteProcess _ -> failwith "invalid task type"
        | CftExpression _ -> failwith "invalid task type"
        | CftPipeline _ -> failwith "invalid task type"
        | CftForEachLoop f -> CftForEachLoop { f with executables = executable::f.executables }
        | CftForLoop f -> CftForLoop { f with executables = executable::f.executables }
        | CftSequence s -> CftSequence { s with executables = executable::s.executables }
    let addExecutables = swapAndFoldList addExecutable

    let getDescription = getTaskBase >> fun tb -> tb.description
    let setDescription description = updateTaskBase <| fun tb -> { tb with description = description }
    let getTransactionOption = getTaskBase >> fun tb -> tb.transactionOption
    let setTransactionOption transactionOption = updateTaskBase <| fun tb -> { tb with transactionOption = transactionOption }
    let getLocaleId = getTaskBase >> fun tb -> tb.localeId
    let setLocaleId localeId = updateTaskBase <| fun tb -> { tb with localeId = localeId }
    let getIsolationLevel = getTaskBase >> fun tb -> tb.isolationLevel
    let setIsolationLevel isolationLevel = updateTaskBase <| fun tb -> { tb with isolationLevel = isolationLevel }
    let getDelayValidation = getTaskBase >> fun tb -> tb.delayValidation
    let setDelayValidation enabled = updateTaskBase <| fun tb -> { tb with delayValidation = enabled }
    let getDisabled = getTaskBase >> fun tb -> tb.disabled
    let setDisabled disabled = updateTaskBase <| fun tb -> { tb with disabled = disabled }
    let getFailParentOnFailure = getTaskBase >> fun tb -> tb.failParentOnFailure
    let setFailParentOnFailure enabled = updateTaskBase <| fun tb -> { tb with failParentOnFailure = enabled }
    let getFailPackageOnFailure = getTaskBase >> fun tb -> tb.failPackageOnFailure
    let setFailPackageOnFailure enabled = updateTaskBase <| fun tb -> { tb with failPackageOnFailure = enabled }
    let getDisableEventHandlers = getTaskBase >> fun tb -> tb.disableEventHandlers
    let setDisableEventHandlers enabled = updateTaskBase <| fun tb -> { tb with disableEventHandlers = enabled }
    let getExpressions = getTaskBase >> fun tb -> tb.expressions
    let setExpressions exprs = updateTaskBase <| fun tb -> { tb with expressions = exprs }
    let getFailOnErrorCountReaching = getTaskBase >> fun tb -> tb.failOnErrorCountReaching
    let setFailOnErrorCountReaching count = updateTaskBase <| fun tb -> { tb with failOnErrorCountReaching = count }
    let getForcedExecutionResult = getTaskBase >> fun tb -> tb.forcedExecutionResult
    let setForcedExecutionResult forcedExecutionResult = updateTaskBase <| fun tb -> { tb with forcedExecutionResult = forcedExecutionResult }
    let getForcedExecutionValue = getTaskBase >> fun tb -> tb.forcedExecutionValue
    let setForcedExecutionValue forcedExecutionValue = updateTaskBase <| fun tb -> { tb with forcedExecutionValue= forcedExecutionValue }
    let getLoggingOptions = getTaskBase >> fun tb -> tb.loggingOptions
    let setLoggingOptions loggingOptions = updateTaskBase <| fun tb -> { tb with loggingOptions = loggingOptions }

    let rec map fnTask fnExeCollection exes =
        let rec map1 exe =
            let mapped = exe |> fnTask
            match mapped with
            | CftSequence s -> CftSequence { s with executables = s.executables |> map fnTask fnExeCollection }
            | CftForEachLoop f -> CftForEachLoop { f with executables = f.executables |> map fnTask fnExeCollection }
            | CftForLoop f -> CftForLoop { f with executables = f.executables |> map fnTask fnExeCollection }
            | CftExecuteSql _ -> mapped
            | CftExecutePackageFromFile _ -> mapped
            | CftExecuteProcess _ -> mapped
            | CftExpression _ -> mapped
            | CftPipeline _ -> mapped
        exes |> List.map map1 |> fnExeCollection

    let rec project (exes : CftExecutable seq) =
        let rec project1 exe =
            seq {
                    yield exe
                    match exe with
                    | CftSequence s -> yield! project (s.executables |> Seq.ofList)
                    | CftForEachLoop f  -> yield! project (f.executables |> Seq.ofList)
                    | CftForLoop f  -> yield! project (f.executables |> Seq.ofList)
                    | CftExecuteSql _ -> ()
                    | CftExecutePackageFromFile _ -> ()
                    | CftExecuteProcess _ -> ()
                    | CftExpression _ -> ()
                    | CftPipeline _ -> ()
                }
        let x = Seq.map project1 exes
        Seq.concat (Seq.map project1 exes)



module PrecedenceConstraints =
    open Executable

    let baseConstraint sourceExecutableName =
        {
            description = ""
            name = ""
            sourceExecutableName = sourceExecutableName
            logic = CfPrecedenceConstraintLogic.Constraint CfExecutableResult.Success
        }
    let createWithConstraint sourceExecutableName condition =
        {
            baseConstraint sourceExecutableName
            with logic = CfPrecedenceConstraintLogic.Constraint condition
        }
        
    let createWithExpression sourceExecutableName expr =
        {
            baseConstraint sourceExecutableName
            with logic = CfPrecedenceConstraintLogic.Expression expr
        }

    let createCompound sourceExecutableName requireBoth expr condition =
        let fn = CfPrecedenceConstraintLogic.ExpressionAndConstraint @?@ CfPrecedenceConstraintLogic.ExpressionOrConstraint
        {
            baseConstraint sourceExecutableName
            with logic = (expr, condition) |> fn requireBoth
        }

    let success = CfExecutableResult.Success
    let failure = CfExecutableResult.Failure
    let completion = CfExecutableResult.Completion

    let successConstraint sourceExecutableName = createWithConstraint sourceExecutableName success
    let failureConstraint sourceExecutableName = createWithConstraint sourceExecutableName failure
    let completionConstraint sourceExecutableName = createWithConstraint sourceExecutableName completion

    let successConstraintN name sourceExecutableName = { successConstraint sourceExecutableName with name = name }
    let failureConstraintN name sourceExecutableName = { failureConstraint sourceExecutableName with name = name }
    let completionConstraintN name sourceExecutableName = { completionConstraint sourceExecutableName with name = name }

    let createWithExpressionN name sourceExecutableName expr = { createWithExpression sourceExecutableName expr with name = name }
    let createCompoundN name sourceExecutableName requireBoth expr condition = { createCompound sourceExecutableName requireBoth expr condition with name = name }

    let chain condition tasks =
        let fn (prior, result) task =
            match prior with
            | None -> task |> getName |> Some, [task]
            | Some name -> 
                let pc = createWithConstraint name condition
                let task' = task |> addPrecedenceConstraint true pc
                task |> getName |> Some, task'::result
        let tasks' = tasks |> List.fold fn (None,[]) |> snd
        match tasks' with
        | [] -> failwith "At least two tasks must be provided to chain"
        | [t] -> failwith "At least two tasks must be provided to chain"
        | t::ts -> t, ts

    let join requireAll condition tasks nextTask =
        let fn state task =
            let pc = createWithConstraint (task |> getName) condition
            state |> addPrecedenceConstraint requireAll pc
        let nextTask' = tasks |> List.fold fn nextTask
        nextTask'

/// API for working with execute sql tasks
module ExecuteSql =
    let private ingress task = match task with CftExecuteSql t -> t | _ -> failwith "Not an ExecuteSqlTask"
    let private egress task = CftExecuteSql task
    let private mutate fn  = ingress >> fn >> egress

    let create name connectionMgr =
        { 
            executableTaskBase = Executable.createTaskBase name

            connection = CfRef.ConnectionManagerRef connectionMgr
            timeoutSeconds = 0
            isStoredProc = false
            bypassPrepare = true

            source = CfDirectSource ""
            codePage = System.Text.Encoding.UTF8.CodePage
            resultType = CfExecuteSqlResult.NoResult

            parameterBindings = []
            resultBindings = []
        }
        |> egress

    let getSqlSource = ingress >> fun t -> t.source
    let setSqlSource source = mutate <| fun t -> { t with source = source }
    let setSqlStatement sql = mutate <| fun t -> { t with source = CfDirectSource sql }
    let setSqlIndirect source = mutate <| fun t -> { t with source = CfIndirectSource source }
    let setSqlFromFile connectionManagerName = setSqlIndirect (CfRef.ConnectionManagerRef connectionManagerName)
    let setSqlFromVariable variableName = setSqlIndirect (CfRef.VariableRef <| Variables.link variableName)

    let getCodePage = ingress >> fun t -> t.codePage
    let setCodePage codePage = mutate <| fun t -> { t with codePage = codePage }

    let getIsStoredProc = ingress >> fun t -> t.isStoredProc
    let setIsStoredProc isStoredProc = mutate <| fun t -> { t with isStoredProc = isStoredProc }

    let getBypassPrepare = ingress >> fun t -> t.bypassPrepare
    let setBypassPrepare bypassPrepare = mutate <| fun t -> { t with bypassPrepare = bypassPrepare }

    let getTimeoutSeconds = ingress >> fun t -> t.timeoutSeconds
    let setTimeoutSeconds timeoutSeconds = mutate <| fun t -> { t with timeoutSeconds = timeoutSeconds }

    let getConnection = ingress >> fun t -> t.connection
    let setConnection connectionManagerName = mutate <| fun t -> { t with connection = CfRef.ConnectionManagerRef connectionManagerName }

    let createSimple name connectionMgr sql =
        create name connectionMgr |> setSqlStatement sql

    /// API for working with execute sql task result bindings
    module ResultBindings =
        let clearResultBindings = mutate <| fun t -> { t with resultBindings = [] }

        let getResultBindings = ingress >> fun sqlTask -> sqlTask.resultBindings
        
        let getResultType resultType = ingress >> fun sqlTask -> sqlTask.resultType
        let setNoResult = mutate <|  fun sqlTask -> { sqlTask with resultType = CfExecuteSqlResult.NoResult ; resultBindings = [] }
        let setResultType resultType = mutate <| fun sqlTask -> { sqlTask with resultType = resultType }

        let addResultBinding resultName variable = mutate <| fun sqlTask -> { sqlTask with resultBindings = (resultName, !@ variable) :: sqlTask.resultBindings }

        let addResultBindings = addResultBinding |> uncurry |> swapAndFoldList 


    /// API for working with execute sql task parameter bindings
    module ParameterBindings =
        let clearParameterBindings = mutate <| fun t -> { t with parameterBindings = [] }

        let getParameterBindings = ingress >> fun sqlTask -> sqlTask.parameterBindings
        let addParameterBinding pb = mutate <| fun sqlTask -> { sqlTask with parameterBindings = pb :: sqlTask.parameterBindings } 
        let addParameterBindings = swapAndFoldList addParameterBinding

        let createParameterBinding name variable direction dataType sizeOption =
            {
                parameterName = name
                targetVariable = !@ variable
                direction = direction
                dataType = dataType
                parameterSize = sizeOption
            }

        let createAndAddParameterBinding name variable direction dataType sizeOption =
            let binding = createParameterBinding name variable direction dataType sizeOption
            addParameterBinding binding

        let getParameterName pb = pb.parameterName
        let getTargetVariable (pb : CfExecuteSqlParameterBinding) = pb.targetVariable
        let getDirection pb = pb.direction
        let getDataType (pb:CfExecuteSqlParameterBinding) = pb.dataType
        let getParameterSize pb = pb.parameterSize

        let setParameterName name pb = { pb with parameterName = name }
        let setTargetVariable variable (pb : CfExecuteSqlParameterBinding) = { pb with targetVariable = Variables.link variable }
        let setDirection direction pb = { pb with direction = direction }
        let setDataType dataType (pb:CfExecuteSqlParameterBinding) = { pb with dataType = dataType }
        let setParameterSize size pb = { pb with parameterSize = size }

/// API for working with execute package tasks.
module ExecutePackage =
    let private ingress task = match task with CftExecutePackageFromFile t -> t | _ -> failwith "Not an ExecutePackageFromFileTask"
    let private egress task = CftExecutePackageFromFile task
    let private mutate fn  = ingress >> fn >> egress

    let create name connectionMgr =
        {
            executableTaskBase = Executable.createTaskBase name

            connection = CfRef.ConnectionManagerRef connectionMgr
            executeOutOfProcess = true
        }
        |> egress

    let getConnection = ingress >> fun t -> t.connection
    let setConnection connectionManagerName = mutate <| fun t -> { t with connection = CfRef.ConnectionManagerRef connectionManagerName }

    let getExecuteOutOfProcess = ingress >> fun t -> t.executeOutOfProcess
    let setExecuteOutOfProcess executeOutOfProcess = mutate <| fun t -> { t with executeOutOfProcess = executeOutOfProcess }

/// API for working with expression tasks.
module Expression =
    let private ingress task = match task with CftExpression t -> t | _ -> failwith "Not an ExpressionTask"
    let private egress task = CftExpression task
    let private mutate fn = ingress >> fn >> egress

    let create name expr =
        {
            executableTaskBase = Executable.createTaskBase name

            expression = CfExpression expr
        }
        |> egress

    let getExpression = ingress >> fun t -> t.expression
    let setExpression expr = mutate <| fun t -> { t with expression = expr }

/// API for working with execute process tasks.
module ExecuteProcess =
    let private ingress task = match task with CftExecuteProcess t -> t | _ -> failwith "Not an ExecuteProcessTask"
    let private egress task = CftExecuteProcess task
    let private mutate fn = ingress >> fn >> egress

    /// Create an execute process task
    let create name =
        {
            executableTaskBase = Executable.createTaskBase name
            targetExecutable = ""
            requireFullFilename = true
            arguments = []
            workingDirectory = ""
            failTaskOnReturnCodeNotEqualToValue = None
            terminateAfterTimeoutSeconds = None
            standardInputVariable = None
            standardOutputVariable = None
            standardErrorVariable = None
            windowStyle = CfWindowStyle.Normal
        }
        |> egress

    let getTargetExecutable = ingress >> fun t -> t.targetExecutable
    let getrequireFullFilename = ingress >> fun t -> t.requireFullFilename
    let getArguments = ingress >> fun t -> t.arguments
    let getWorkingDirectory = ingress >> fun t -> t.workingDirectory
    let getFailTaskOnReturnCodeNotEqualToValue = ingress >> fun t -> t.failTaskOnReturnCodeNotEqualToValue
    let getTerminateAfterTimeoutSeconds = ingress >> fun t -> t.terminateAfterTimeoutSeconds
    let getStandardInputVariable = ingress >> fun t -> t.standardInputVariable
    let getStandardOutputVariable = ingress >> fun t -> t.standardOutputVariable
    let getStandardErrorVariable = ingress >> fun t -> t.standardErrorVariable
    let getWindowStyle = ingress >> fun t -> t.windowStyle

    let setTargetExecutable targetExecutable = mutate <| fun t -> { t with targetExecutable = targetExecutable }
    let setRequireFullFilename requireFullFilename = mutate <| fun t -> { t with requireFullFilename = requireFullFilename }
    let setArguments arguments = mutate <| fun t -> { t with arguments = arguments }
    let setWorkingDirectory workingDirectory = mutate <| fun t -> { t with workingDirectory = workingDirectory }
    let setFailTaskOnReturnCodeNotEqualToValue value = mutate <| fun t -> { t with failTaskOnReturnCodeNotEqualToValue = value }
    let setTerminateAfterTimeoutSeconds timeoutSeconds = mutate <| fun t -> { t with terminateAfterTimeoutSeconds = timeoutSeconds }
    let setStandardInputVariable standardInputVariable = mutate <| fun t -> { t with standardInputVariable = standardInputVariable }
    let setStandardOutputVariable standardOutputVariable = mutate <| fun t -> { t with standardOutputVariable = standardOutputVariable }
    let setStandardErrorVariable standardErrorVariable = mutate <| fun t -> { t with standardErrorVariable = standardErrorVariable }
    let setWindowStyle windowStyle = mutate <| fun t -> { t with windowStyle = windowStyle }

/// API for working with sequence tasks.
module Sequence =

    let private assertType task : CftExecuteSequence = task
    let private ingress task = match task with CftSequence t -> t | _ -> failwith "Not a Sequence"
    let private egress = CftSequence
    let private mutate fn = ingress >> fn >> egress
    
    /// Create an empty sequence task
    let create name =
        egress
        <| {
               executableTaskBase = Executable.createTaskBase name
               executables = []
           }
    /// Get the executables
    let getExecutables = assertType >> fun task -> task.executables

    /// Add an executable
    let addExecutable e task =
        let adder e = assertType >> fun t -> { t with executables = e :: t.executables } 
        let tryAdd' = tryAdd getExecutables Executable.getName adder stringCompareInvariantIgnoreCase
        mutate (tryAdd' e) task
    /// Add multiple executables
    let addExecutables = swapAndFoldList addExecutable
    /// Remove all executables
    let clearExecutables = mutate (fun t -> { t with executables = [] })

/// API for working with pipeline tasks.  See also the data flow API namespace.
module Pipeline =

    let private ingress task = match task with CftPipeline t -> t | _ -> failwith "Not a Pipeline"
    let private egress = CftPipeline
    let private mutate fn = ingress >> fn >> egress
    
    /// Create an empty pipeline task
    let create name =
      egress
      <| { 
            executableTaskBase = Executable.createTaskBase name 
            model = { components = [] }
            defaultBufferMaxRows = None
            defaultBufferSize = None
            engineThreadsHint = None
            blobTempStoragePath = None
            bufferTempStoragePath = None
            runInOptimizedMode = None
         }

    /// Get the model
    let getModel = ingress >> fun p -> p.model
    /// Set the model
    let setModel model = mutate <| fun p -> { p with model = model }

    /// Get the pipeline components
    let getComponents = ingress >> fun p -> p.model.components
    /// Clear the pipeline components
    let clearComponents = mutate <| fun p -> { p with model = { p.model with components = [] } }
    /// Add a pipeline component
    let addComponent c = mutate <| fun p -> { p with model = { p.model with components = c::p.model.components }}
    /// Add multiple pipeline components
    let addComponents = swapAndFoldList addComponent

    /// Get the default buffer max rows value
    let getDefaultBufferMaxRows = ingress >> fun t -> t.defaultBufferMaxRows
    /// Get the default buffer size
    let getDefaultBufferSize = ingress >> fun t -> t.defaultBufferSize
    /// Get the engine threads hint
    let getEngineThreadsHint = ingress >> fun t -> t.engineThreadsHint
    /// Get the temporary storage path for blobs
    let getBlobTempStoragePath = ingress >> fun t -> t.blobTempStoragePath
    /// Get the temporary storage path for buffers
    let getBufferTempStoragePath = ingress >> fun t -> t.bufferTempStoragePath
    /// Get the run in optimized mode flag
    let getRunInOptimizedMode = ingress >> fun t -> t.runInOptimizedMode

    /// Set the default buffer max rows value
    let setDefaultBufferMaxRows defaultBufferMaxRows = mutate <| fun t -> { t with defaultBufferMaxRows = defaultBufferMaxRows }
    /// Set the default buffer size
    let setDefaultBufferSize defaultBufferSize = mutate <| fun t -> { t with defaultBufferSize = defaultBufferSize }
    /// Set the engine threads hint
    let setEngineThreadsHint engineThreadsHint = mutate <| fun t -> { t with engineThreadsHint = engineThreadsHint}
    /// Set the temporary storage path for blobs
    let setBlobTempStoragePath blobTempStoragePath = mutate <| fun t -> { t with blobTempStoragePath = blobTempStoragePath}
    /// Set the temporary storage path for buffers
    let setBufferTempStoragePath bufferTempStoragePath = mutate <| fun t -> { t with bufferTempStoragePath = bufferTempStoragePath }
    /// Set the run in optimized mode flag
    let setRunInOptimizedMode runInOptimizedMode = mutate <| fun t -> { t with runInOptimizedMode = runInOptimizedMode }

    /// Gets a pipeline component by name
    let getComponent name = getComponents >> List.find (PipelineCommon.get_name >> stringCompareInvariant name)

module ForEachLoop =
  
    let private assertType task : CftForEachLoop = task
    let private ingress task = match task with CftForEachLoop t -> t | _ -> failwith "Not a ForEachLoop"
    let private egress = CftForEachLoop
    let private mutate fn = ingress >> fn >> egress

    /// Create an empty FOR EACH LOOP task
    let create name enumeratorLogic =
      egress
      <| { 
            executableTaskBase = Executable.createTaskBase name 
            executables = []
            enumerationExpressions = []
            enumeratorLogic = enumeratorLogic
            variableMappings = []
         }

    /// Get the executables
    let getExecutables = assertType >> fun task -> task.executables

    /// Add an executable
    let addExecutable e task =
        let adder e = assertType >> fun t -> { t with executables = e :: t.executables } 
        let tryAdd' = tryAdd getExecutables Executable.getName adder stringCompareInvariantIgnoreCase
        mutate (tryAdd' e) task
    /// Add multiple executables
    let addExecutables = swapAndFoldList addExecutable
    /// Remove all executables
    let clearExecutables = mutate (fun t -> { t with executables = [] })

    let getEnumeratorLogic = ingress >> fun t -> t.enumeratorLogic
    let setEnumeratorLogic enumeratorLogic = mutate <| fun t -> { t with enumeratorLogic = enumeratorLogic }

    let clearVariableMappings = mutate <| fun t -> { t with variableMappings = [] }
    let getVariableMappings = ingress >> fun t -> t.variableMappings
    let addVariableMapping vm = mutate <| fun t-> { t with variableMappings = t.variableMappings @ [vm] } // preserve order
    let addVariableMappings vms = mutate <| fun t-> { t with variableMappings = t.variableMappings @ vms } // preserve order

    let createVariableMapping variableName exprs = { CfForEachLoopVariableMapping.target = Variables.link variableName ; expressions = exprs }

    let clearEnumerationExpressions = mutate <| fun t -> { t with enumerationExpressions = [] }
    let getEnumerationExpressions = ingress >> fun t -> t.enumerationExpressions
    let addEnumerationExpression vm = mutate <| fun t-> { t with enumerationExpressions = vm :: t.enumerationExpressions } 
    let addEnumerationExpressions = swapAndFoldList addEnumerationExpression

/// API for working with FOR LOOP tasks
module ForLoop =

    let private assertType task : CftForLoop = task
    let private ingress task = match task with CftForLoop t -> t | _ -> failwith "Not a ForLoop"
    let private egress = CftForLoop
    let private mutate fn = ingress >> fn >> egress

    /// Create an empty FOR LOOP task
    let create name initExpression evalExpression assignExpression =
      egress
      <| { 
            executableTaskBase = Executable.createTaskBase name 
            executables = []
            initExpression = initExpression
            evalExpression = evalExpression
            assignExpression = assignExpression
         }

    /// Get the executables
    let getExecutables = assertType >> fun task -> task.executables

    /// Add an executable
    let addExecutable e task =
        let adder e = assertType >> fun t -> { t with executables = e :: t.executables } 
        let tryAdd' = tryAdd getExecutables Executable.getName adder stringCompareInvariantIgnoreCase
        mutate (tryAdd' e) task
    /// Add multiple executables
    let addExecutables = swapAndFoldList addExecutable
    /// Remove all executables
    let clearExecutables = mutate (fun t -> { t with executables = [] })

    /// Get the loop initialisation expression
    let getInitExpression = ingress >> fun t -> t.initExpression
    /// Get the post-loop iteration assignment expression
    let getAssignExpression = ingress >> fun t -> t.assignExpression
    /// Get the loop condition expression
    let getEvalExpression = ingress >> fun t -> t.evalExpression

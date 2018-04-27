namespace Chimayo.Ssis.Ast.ControlFlow

/// The top-level SSIS package type
type CftPackage =
    {
        name: string
        creationDate: System.DateTimeOffset option

        connectionManagers: CfConnectionManager list
        configurations: CfPackageConfiguration list
        logProviders: CfLogProvider list
        variables: CfVariable list

        loggingOptions: CfLoggingOptions

        propertyExpressions: CfPropertyExpressions
        executables: CftExecutable list

        maxConcurrentExecutables: int
        enableConfigurations: bool

        forcedExecutionValue: CfData option
        forcedExecutionResult: CfExecutableResult option
        disabled: bool
        failParentOnFailure: bool
        failOnErrorCountReaching: int
        isolationLevel: CfIsolationLevel
        localeId: int
        transactionOption: CfTransactionOption
        delayValidation: bool
        disableEventHandlers: bool

        parameters: pkParameter list

    }
    /// Default empty package
    static member Empty =
                           {
                               name = "Unnamed package"
                               creationDate = System.DateTimeOffset.Now |> Some

                               connectionManagers = []
                               configurations = []
                               logProviders = []
                               variables = []
       
                               loggingOptions = 
                                   { 
                                       filterKind = CfLogFilterKind.ByInclusion
                                       loggingMode = CfLogMode.Disabled
                                       logProviders = []
                                       logSelections = []
                                   }

                               propertyExpressions = []
                               executables = []

                               maxConcurrentExecutables = 0
                               enableConfigurations = true

                               forcedExecutionValue = None
                               forcedExecutionResult = None
                               disabled = false
                               failParentOnFailure = false
                               failOnErrorCountReaching = 1
                               isolationLevel = CfIsolationLevel.Serializable
                               localeId = System.Globalization.CultureInfo.InvariantCulture.LCID
                               transactionOption = CfTransactionOption.Supported
                               delayValidation = false
                               disableEventHandlers = false

                               parameters = []
                           }


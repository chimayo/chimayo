namespace Chimayo.Ssis.Ast.ControlFlowApi

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Ast.ControlFlow

/// API to support building common SQL Server connection string values and expressions
module SqlConnectionStringHelper =
    /// Helper type representing the individual components of a SQL Server connection string
    type SqlConnectionString =
        {
            server : string
            database : string
            appName : string
            useIntegratedSecurity : bool
            username: string
            password : string
            provider : string
        }

    /// An empty SqlConnectionString value
    let empty = 
        {
            server = ""
            database = ""
            appName = ""
            useIntegratedSecurity = true
            username = ""
            password = ""
            provider = ""
        }
    
    /// Transform a SqlConnectionString by changing the database name
    let setDatabase dbName scs = { scs with database = dbName }
    /// Transform a SqlConnectionString by changing the server name
    let setServer svr scs = { scs with server = svr }
    /// Transform a SqlConnectionString by changing the application name
    let setAppName appName scs = { scs with appName = appName }
    /// Transform a SqlConnectionString by enabling integrated security
    let useIntegratedSecurity scs = { scs with useIntegratedSecurity = true ; username = "" ; password = "" }
    /// Transform a SqlConnectionString by embedding username/password credentials
    let useUsernamePasswordSecurity uname pwd scs = { scs with useIntegratedSecurity = false ; username = uname ; password = pwd }
    /// Transform a SqlConnectionString by setting the provider to the OLE DB SQL Server 2012 Native Client provider
    let setProviderToSqlNativeClient11 scs = { scs with provider = "SQLNCLI11.1" }
    
    /// Produces a connection string by applying a filter to values that may need to be escaped and concatenating the elements
    let private toGenericConnectionString filter scs =
        let security =
            scs.useIntegratedSecurity 
            |> "Integrated Security=SSPI" 
               @?@ (sprintf "UID=%s;PWD=%s" (filter scs.username) (filter scs.password))
        sprintf "Provider=%s;Data Source=%s;Initial Catalog=%s;Application Name=%s;%s;"
            (filter scs.provider)
            (filter scs.server)
            (filter scs.database)
            (filter scs.appName)
            security

    /// Produce an OLE DB compatible connection string
    let toOleDbConnectionString = 
        let filter (x:string) =
            sprintf "\"%s\"" (x.Replace("\"", "\"\""))
        toGenericConnectionString filter

    /// Produce an ADO compatible connection string
    let toAdoConnectionString = 
        toGenericConnectionString id

    /// Produce an ADO.NET compatible connection string
    let toAdoNetConnectionString =
        toGenericConnectionString id

    /// Create an OLE DB connection string
    let createInlineOleDbConnectionString svr dbName upOption appName =
        let hasUp = upOption |> Option.isSome
        let delay value = fun () -> value
        let applyUp = fun () -> (uncurry useUsernamePasswordSecurity) (upOption |> Option.get)
        let upfn = applyUp @?@ (delay useIntegratedSecurity) <| hasUp
        empty
        |> setServer svr
        |> setDatabase dbName
        |> upfn ()
        |> setAppName appName
        |> setProviderToSqlNativeClient11
        |> toOleDbConnectionString

    /// Create an ADO.NET connection string
    let createInlineAdoNetConnectionString svr dbName upOption appName =
        let hasUp = upOption |> Option.isSome
        let delay value = fun () -> value
        let applyUp = fun () -> (uncurry useUsernamePasswordSecurity) (upOption |> Option.get)
        let upfn = applyUp @?@ (delay useIntegratedSecurity) <| hasUp
        empty
        |> setServer svr
        |> setDatabase dbName
        |> upfn ()
        |> setAppName appName
        |> toAdoNetConnectionString

    /// <summary>Create an expression that can be used to specify an OLE DB connection string.
    /// <para>The server, database and application name values can be supplied as string
    /// values or as CfVariableRef values, in which case the variable value is injected.
    /// </para></summary>
    let createOleDbConnectionStringExpression (svr:#obj) (dbName:#obj) upOption (appName:#obj) =
        let extract (a:#obj) = 
            match a:>obj with 
            | :? CfVariableRef as x -> true, sprintf "@[%s::%s]" x.``namespace`` x.name 
            | :? string as x -> false, x.Replace("\"", "\\\"\\\"")
            | _ -> failwith "Invalid value"

        let build v =
            let (isVar, value) = extract v
            if isVar then sprintf "\" + %s + \"" value else value

        let inject placeholder value (text:string) = text.Replace(placeholder, build value)

        createInlineOleDbConnectionString "**SERVER**" "**DBNAME**" upOption "**APPNAME**" // create a placeholder variant
        |> fun x -> x.Replace("\"", "\\\"") // quote internal characters
        |> sprintf "\"%s\"" // surround with quotes
        |> inject "**SERVER**" svr
        |> inject "**DBNAME**" dbName
        |> inject "**APPNAME**" appName

    /// <summary>Create an expression that can be used to specify an ADO.NET connection string.
    /// <para>The server, database and application name values can be supplied as string
    /// values or as CfVariableRef values, in which case the variable value is injected.
    /// </para></summary>
    let createAdoNetConnectionStringExpression (svr:#obj) (dbName:#obj) upOption (appName:#obj) =
        let extract (a:#obj) = 
            match a:>obj with 
            | :? CfVariableRef as x -> true, sprintf "@[%s::%s]" x.``namespace`` x.name 
            | :? string as x -> false, x.Replace("\"", "\\\"\\\"")
            | _ -> failwith "Invalid value"

        let build v =
            let (isVar, value) = extract v
            if isVar then sprintf "\" + %s + \"" value else value

        let inject placeholder value (text:string) = text.Replace(placeholder, build value)

        createInlineAdoNetConnectionString "**SERVER**" "**DBNAME**" upOption "**APPNAME**" // create a placeholder variant
        |> fun x -> x.Replace("\"", "\\\"") // quote internal characters
        |> sprintf "\"%s\"" // surround with quotes
        |> inject "**SERVER**" svr
        |> inject "**DBNAME**" dbName
        |> inject "**APPNAME**" appName


/// API to support working with variables
module Variables =

    /// Get the namespace and name of a variable
    let getQualifiedName (variable: CfVariable) = variable.``namespace``, variable.name
    /// Get the value of a variable
    let getValue (variable: CfVariable) = variable.value
    /// Get the data type of a variable
    let getDataType (variable: CfVariable) = variable.value |> CfData.getType
    /// Get the expression assigned to a variable
    let getExpression (variable: CfVariable) = variable.expression

    /// <summary>Translates a string of the form '[namespace::]name' to an instance of CfVariableRef.
    /// <para>If a namespace is not provided, the namespace 'User' is used.</para>
    /// </summary>
    let link = CfVariableRef.fromString

    /// Translates a CfVariableRef to the form 'namespace::name'
    let scopedReferenceToString = CfVariableRef.toString

    /// Translates a string of the form '[namespace::]name' to 'namespace'. The namespace 'User' is returned if none is provided.
    let getNamespaceFromQualifiedName qualifiedName = qualifiedName |> link |> fun s -> s.``namespace``
    /// Translates a string of the form '[namespace::]name' to 'name'.
    let getNameFromQualifiedName qualifiedName = qualifiedName |> link |> fun s -> s.name

    /// Construct a new CfVariable value with explicit values for all properties
    let createWithDataValue ``namespace`` name isReadOnly raiseChangedEvent value expr =
        let expr' = expr |> Option.map CfExpression
        {
            ``namespace`` = ``namespace``
            name = name
            value = value
            expression = expr'
            isReadOnly = isReadOnly
            raiseChangedEvent = raiseChangedEvent
        }

    /// Construct a new CfVariable value where the value construction is delegated to CfData.create
    let create ``namespace`` name isReadOnly raiseChangedEvent value expr= 
        createWithDataValue ``namespace`` name isReadOnly raiseChangedEvent (CfData.create value) expr

    /// Construct a new CfVariable value with the following properties: read-write, no expression, do not raise changed event.
    let createSimple qualifiedName value =
        let svr = link qualifiedName
        create svr.``namespace`` svr.name false false value None

    /// Produces the fully qualified SSIS path to a specific variable
    let getAbsoluteName executableNameList (qualifiedName : CfVariableRef) =
        let parents = "Package" :: executableNameList
        let ns, name = qualifiedName.``namespace``, qualifiedName.name
        sprintf "\\%s.Variables[%s::%s]" (System.String.Join("\\", parents)) ns name

    /// Transform a CfVariable by changing the value
    let setValue value v = { v with value = value } : CfVariable
    /// Transform a CfVariable by removing any expression
    let clearExpression v = { v with expression = None } : CfVariable
    /// Transform a CfVariable by providing an expression
    let setExpression expr v = { v with expression = expr |> CfExpression |> Some } : CfVariable
    /// Get the value of the read only flag
    let isReadOnly (v:CfVariable) = v.isReadOnly
    /// Get the value of the raises changed event flag
    let raisesChangedEvent (v:CfVariable) = v.raiseChangedEvent
    /// Get the name of the variable
    let getName (v:CfVariable) = v.name
    /// Transform a CfVariable by providing a new namespace and name
    let setName nameAndNamespace (v:CfVariable) = 
        let ns,name = nameAndNamespace |> CfVariableRef.fromString |> fun x -> x.``namespace``, x.name
        { v with name = name ; ``namespace`` = ns }
    /// Transform a CfVariable by setting the read only flag
    let setReadOnly enabled (v:CfVariable) = { v with isReadOnly = enabled }
    /// Transform a CfVariable by setting the raise changed event flag
    let setRaiseChangedEvent enabled (v:CfVariable) = { v with raiseChangedEvent = enabled }

/// API to support working with parameters
module Parameters =

    /// Get the namespace and name of a parameter
    let getQualifiedName (parameter: pkParameter) = parameter.``namespace``, parameter.name
    /// Get the value of a parameter
    let getValue (parameter: pkParameter) = parameter.value
    /// Get the data type of a parameter
    let getDataType (parameter: pkParameter) = parameter.value |> CfData.getType
    /// <summary>Translates a string of the form '[namespace::]name' to an instance of CfParameterRef.
    /// <para>If a namespace is not provided, the namespace '$Package' is used.</para>
    /// </summary>
    let link = CfParemeterRef.fromString

    /// Translates a CfParameterRef to the form 'namespace::name'
    let scopedReferenceToString = CfParemeterRef.toString

    /// Translates a string of the form '[namespace::]name' to 'namespace'. The namespace '$Package' is returned if none is provided.
    let getNamespaceFromQualifiedName qualifiedName = qualifiedName |> link |> fun s -> s.``namespace``
    /// Translates a string of the form '[namespace::]name' to 'name'.
    let getNameFromQualifiedName qualifiedName = qualifiedName |> link |> fun s -> s.name

    /// Construct a new CfParameter value with explicit values for all properties
    let createWithDataValue ``namespace`` name value isRequired isSensitive =
        {
            ``namespace`` = ``namespace``
            name = name
            value = value
            isRequired = isRequired
            isSensitive = isSensitive
        }

    /// Construct a new CfParameter value where the value construction is delegated to CfData.create
    let create ``namespace`` name value isRequired isSensitive= 
        createWithDataValue ``namespace`` name (CfData.create value) isRequired isSensitive

    /// Construct a new CfParameter value with the following properties: not required, not sensitive
    let createSimple qualifiedName value =
        let spr = link qualifiedName
        create spr.``namespace`` spr.name value false false

    ///// Produces the fully qualified SSIS path to a specific parameter
    //let getAbsoluteName executableNameList (qualifiedName : CfParemeterRef) =
    //    let parents = "Package" :: executableNameList
    //    let ns, name = qualifiedName.``namespace``, qualifiedName.name
    //    sprintf "\\%s.Parameters[%s::%s]" (System.String.Join("\\", parents)) ns name

    /// Transform a pkParameter by changing the value
    let setValue value v = { v with value = value } : pkParameter
    /// Get the value of the required flag
    let isRequired (v:pkParameter) = v.isRequired
    /// Get the value of the raises changed event flag
    let isSensitive (v:pkParameter) = v.isSensitive
    /// Get the name of the parameter
    let getName (v:pkParameter) = v.name
    /// Transform a pkParameter by providing a new namespace and name
    let setName nameAndNamespace (v:pkParameter) = 
        let ns,name = nameAndNamespace |> CfParemeterRef.fromString |> fun x -> x.``namespace``, x.name
        { v with name = name ; ``namespace`` = ns }
    /// Transform a pkParameter by setting the read only flag
    let setRequired enabled (v:pkParameter) = { v with isRequired = enabled }
    /// Transform a pkParameter by setting the raise changed event flag
    let setSensitive enabled (v:pkParameter) = { v with isSensitive = enabled }

/// API to support working with property expressions
module Expressions =
    /// Create a new property expression definition
    let createPropertyExpression target expr = 
        {
            targetProperty = target
            expression = (CfExpression expr)
        }
    /// Get the target property
    let getTargetProperty (expr:CfPropertyExpression) = expr.targetProperty
    /// Get the expression text from a CfExpression
    let getExpressionText (CfExpression e) = e
    /// Get the expression text from a CfPropertyExpression
    let getExpressionPropertyText (expr:CfPropertyExpression) = expr.expression |> getExpressionText
    /// Add a new property expression to a list of existing property expressions
    let addExpression (expr:CfPropertyExpression) exprs =
        let exists = exprs |> List.tryFind (fun x -> stringCompareInvariantIgnoreCase (x |> getTargetProperty) expr.targetProperty) |> Option.isSome
        do exists @?! "Property expression already defined"
        expr::exprs
    /// Create a new property expression definition and add it to a list of existing property expressions
    let addPropertyExpressionDirect target expr =
        createPropertyExpression target expr |> addExpression

/// API to support working with log providers
module LogProviders =
    /// Get the name of the log provider
    let getName = 
        function
        | CfSqlLogProvider slp -> slp.name

    /// Create a new SQL Server log provider definition
    let createSqlLogProvider name conn delayValidation =
        CfSqlLogProvider
            {
                name = name
                connection = CfRef.ConnectionManagerRef conn
                delayValidation = delayValidation
            }

/// API to support working with logging options
module LoggingOptions =
    /// Create a new logging options definition
    let create loggingMode filterKind providers =
        {
            filterKind = filterKind
            loggingMode = loggingMode
            logProviders = providers
            logSelections = []
        }

    /// Default logging options: use parent, filter by exclusion, no log events specified
    let defaults = create CfLogMode.UseParentSetting CfLogFilterKind.ByExclusion []

    /// Transfomr a logging options definition by supplying log selections
    let configureLogging logSelections loggingOptions =
        {
            loggingOptions with logSelections = logSelections
        }

    /// Create log selections for the cartesian product of the supplied events and columns
    let createLogSelections events columns = events |> List.map (fun eventName -> { eventName = eventName ; columns = columns } )

/// API to support working with package configurations
module PackageConfigurations =

    /// Get the name of the package configuration
    let getName = function
        | CfParentVariablePackageConfiguration cfg -> cfg.name

    /// Transform a package configuration by changing the name
    let setName name = function
        | CfParentVariablePackageConfiguration cfg -> CfParentVariablePackageConfiguration { cfg with name = name }

    /// Get the description of a package configuration
    let getDescription = function
        | CfParentVariablePackageConfiguration cfg -> cfg.description

    /// Transform a package configuration by changing the description
    let setDescription description = function
        | CfParentVariablePackageConfiguration cfg -> CfParentVariablePackageConfiguration { cfg with description = description }

    /// Create a new package configuration using parent variable semantics.    
    let createParentVariableConfiguration name parentVariableQualifiedName ignoreParentVariableNamespace targetProperty =
        CfParentVariablePackageConfiguration
            {
                description = ""
                name = name
                expressions = []
                parentVariableName = parentVariableQualifiedName
                ignoreParentVariableNamespace = ignoreParentVariableNamespace
                targetProperty = targetProperty
            }

    /// Create a new package configuration using parent variable semantics where the configuration is used to set the value
    /// of the corresponding variable in the child package.
    let createSimpleParentVariableConfiguration (variableName:string) ignoreParentVariableNamespace =
        createParentVariableConfiguration
            (variableName.Replace(":","_"))
            (Variables.link variableName)
            ignoreParentVariableNamespace
            (sprintf "%s.Properties[Value]" (Variables.getAbsoluteName [] (Variables.link variableName)))

    /// Create a new package configuration using parent variable semantics where the configuration is used to set the value
    /// of the corresponding variable in the child package.  Variant allows defining the name of the configuration explicitly.
    let createSimpleParentVariableConfigurationWithName name (variableName:string) ignoreParentVariableNamespace =
        createParentVariableConfiguration
            name
            (Variables.link variableName)
            ignoreParentVariableNamespace
            (sprintf "%s.Properties[Value]" (Variables.getAbsoluteName [] (Variables.link variableName)))

/// API for working with specific properties of parent variable package configurations
module ParentVariablePackageConfiguration =
    /// Extracts configuration data
    let private ingress = function | CfParentVariablePackageConfiguration cfg -> cfg
    /// Wraps configuration data in a CfPackageConfiguration
    let private egress = CfParentVariablePackageConfiguration
    /// Performs a mapping on configuration data
    let private mutate fn = ingress >> fn >> egress

    /// Get the value of the ignore parent variable namespace flag.  When set, only the name of the parent variable is stored in the package configuration and
    /// the SSIS runtime will match variables in alternate namespaces.
    let getIgnoreParentVariableNamespace = ingress >> fun c -> c.ignoreParentVariableNamespace
    /// Set the value of the ignore parent variable namespace flag.  When set, only the name of the parent variable is stored in the package configuration and
    /// the SSIS runtime will match variables in alternate namespaces.
    let setIgnoreParentVariableNamespace ignoreParentVariableNamespace = mutate <| fun c -> { c with ignoreParentVariableNamespace = ignoreParentVariableNamespace }

    /// Get the SSIS path to the property that will be overwritten by the package configuration.
    let getTargetProperty = ingress >> fun c -> c.targetProperty
    /// Set the SSIS path to the property that will be overwritten by the package configuration.
    let setTargetProperty targetProperty = mutate <| fun c -> { c with targetProperty = targetProperty}

    /// Get the parent variable
    let getParentVariable = ingress >> fun c -> c.parentVariableName
    /// Set the parent variable
    let setParentVariable parentVariable = mutate <| fun c -> { c with parentVariableName = Variables.link parentVariable }


[<AutoOpen>]
module CustomOperators =
    /// <summary>Translates a string of the form '[namespace::]name' to an instance of CfVariableRef.
    /// <para>If a namespace is not provided, the namespace 'User' is used.</para>
    /// </summary>
    let (!@) = Variables.link

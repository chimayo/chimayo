namespace Chimayo.Ssis.Ast.ControlFlow

/// Control flow data types
type CfDataType =
        | Empty = 0
        | Null = 1
        | Int16 = 2
        | Int32 = 3
        | Real32 = 4
        | Real64 = 5
        | Currency = 6
        | Date = 7
        | String = 8
        | Boolean = 11
        | Object = 13
        | Decimal = 14
        | Int8 = 16
        | UInt8 = 17
        | UInt16 = 18
        | UInt32 = 19
        | Int64 = 20
        | UInt64 = 21
        | IntNoSize = 22
        | UIntNoSize = 23
        /// Number of 100 nanosecond units since the start of January 1, 1601
        | Timestamp64 = 64 
        | Guid = 72
        | VarString = 129
        /// Max length 8000 characters
        | NullTerminatedUnicodeString = 130 
        | Numeric = 131
        | DbDate = 133
        | DbTime = 134
        | DbTimeStamp = 135
        | Varnumeric = 139
        | DbTimeExtended = 145
        | DbTimeStampOffset = 146

/// Control flow typed data value
module _CfData =
    /// Control flow typed data value
    type T =
        | Empty
        | Null
        | Int8 of int8
        | Int16 of int16
        | Int32 of int32
        | Int64 of int64
        | UInt8 of uint8
        | UInt16 of uint16
        | UInt32 of uint32
        | UInt64 of uint64
        | Real32 of float32
        | Real64 of float
        | Currency of decimal
        | Date of System.DateTime
        | String of string
        | Boolean of bool
        /// Values of type object are not supported, but the data type is to support runtime behaviour
        | Object 
        | Decimal of decimal
        | Guid of System.Guid
        | Numeric of decimal
        
        /// <summary>Creates a new instance of CfData by a typed .NET object.
        /// <para>Not all data types can be created this way due to ambiguity.
        /// 'None' is converted to 'Null'.
        /// 'unit' is converted to 'Empty'.</para>
        /// </summary>
        static member create<'a> (o:'a) = 
            let rec create' o =
                match o:>obj with
                | :? unit -> Empty
                | :? ('a option) as x when Option.isNone x -> Null
                | :? ('a option) as x -> x |> Option.get |> create'
                | :? string as s -> String s
                | :? int8 as v -> Int8 v
                | :? int16 as v -> Int16 v
                | :? int32 as v -> Int32 v
                | :? int64 as v -> Int64 v
                | :? uint8 as v -> UInt8 v
                | :? uint16 as v -> UInt16 v
                | :? uint32 as v -> UInt32 v
                | :? uint64 as v -> UInt64 v
                | :? float32 as v -> Real32 v
                | :? float as v -> Real64 v
                | :? decimal as v -> Decimal v
                | :? System.DateTime as v -> Date v
                | :? bool as b -> Boolean b
                | :? System.Guid as g -> Guid g
                | _ -> failwith "Unsupported data type construction value"
            create' o

        /// Gets the CfDataType of a CfData value
        static member getType value =
            match value with
            | Empty -> CfDataType.Empty
            | Null -> CfDataType.Null
            | Int8 _ -> CfDataType.Int8
            | Int16 _ -> CfDataType.Int16
            | Int32 _ -> CfDataType.Int32
            | Int64 _ -> CfDataType.Int64
            | UInt8 _ -> CfDataType.UInt8
            | UInt16 _ -> CfDataType.UInt16
            | UInt32 _ -> CfDataType.UInt32
            | UInt64 _ -> CfDataType.UInt64
            | Real32 _ -> CfDataType.Real32
            | Real64 _ -> CfDataType.Real64
            | Currency _ -> CfDataType.Currency
            | Date _ -> CfDataType.Date
            | String _ -> CfDataType.String
            | Boolean _ -> CfDataType.Boolean
            | Object -> CfDataType.Object
            | Decimal _ -> CfDataType.Decimal
            | Guid _ -> CfDataType.Guid
            | Numeric _ -> CfDataType.Numeric

        /// Gets a default CfData value for a specific CfDataType
        static member  getDefault (t:CfDataType) =
            match t with
            | CfDataType.Empty -> Empty
            | CfDataType.Null -> Null
            | CfDataType.Int16 -> Int16 0s
            | CfDataType.Int32 -> Int32 0
            | CfDataType.Real32 -> Real32 0.0f
            | CfDataType.Real64 -> Real64 0.0
            | CfDataType.Currency -> Currency 0m
            | CfDataType.Date -> Date System.DateTime.UtcNow
            | CfDataType.String -> String ""
            | CfDataType.Boolean -> Boolean false
            | CfDataType.Object -> Object 
            | CfDataType.Decimal -> Decimal 0m
            | CfDataType.Int8 -> Int8 0y
            | CfDataType.UInt8 -> UInt8 0uy
            | CfDataType.UInt16 -> UInt16 0us
            | CfDataType.UInt32 -> UInt32 0u
            | CfDataType.Int64 -> Int64 0L
            | CfDataType.UInt64 -> UInt64 0UL
            | CfDataType.Guid -> Guid (System.Guid.Empty)
            | CfDataType.Numeric -> Numeric 0m
            | _ -> failwith "Not supported"

/// Control flow typed data value
type CfData = _CfData.T

/// Control flow parameter direction
module _CfParameterDirection =
    /// Control flow parameter direction
    type T =
        | InputParameter
        | OutputParameter
        | ReturnValue
        /// Interprets the internal SSIS textual value to a discriminated union case
        static member fromString = function
            | "Input" -> InputParameter
            | "Output" -> OutputParameter
            | "ReturnValue" -> ReturnValue
            | _ -> failwith "Invalid value supplied"
        /// Converts the discriminated union case to the internal SSIS textual value
        static member toString = function
            | InputParameter -> "InputParameter"
            | OutputParameter -> "OutputParameter"
            | ReturnValue -> "ReturnValue"

/// Control flow parameter direction
type CfParameterDirection = _CfParameterDirection.T

/// Defines the kind of result produced by a query or statement as part of the ExecuteSql task
module _CfExecuteSqlResult =
    /// Defines the kind of result produced by a query or statement as part of the ExecuteSql task
    type T =
        | NoResult
        | RowsetResult
        | SingleRowResult
        | XmlResult

        /// Interprets the internal SSIS textual value to a discriminated union case
        static member fromString = function
            | "ResultSetType_None" -> NoResult
            | "ResultSetType_Rowset" -> RowsetResult
            | "ResultSetType_SingleRow" -> SingleRowResult
            | "ResultSetType_XML" -> XmlResult
            | _ -> failwith "Invalid value supplied"
        /// Converts the discriminated union case to the internal SSIS textual value
        static member toString = function
            | NoResult -> "ResultSetType_None"
            | RowsetResult -> "ResultSetType_Rowset"
            | SingleRowResult -> "ResultSetType_SingleRow"
            | XmlResult -> "ResultSetType_XML"

/// Defines the kind of result produced by a query or statement as part of the ExecuteSql task
type CfExecuteSqlResult = _CfExecuteSqlResult.T

/// <summary>Represents an SSIS expression</summary>
/// <remarks>Note that expressions are not parsed at this time</remarks>
type CfExpression =
    CfExpression of string

/// Represents the assignment of an expression to an object property
type CfPropertyExpression =
    {
        targetProperty: string
        expression: CfExpression
    }

/// List of SSIS expression ot object property assignments
type CfPropertyExpressions = CfPropertyExpression list

/// Reference to an SSIS variable
type CfVariableRef =
    {
        ``namespace``: string
        name: string
    }
    /// <summary>Translates a string of the form '[namespace::]name' to an instance of CfVariableRef.
    /// <para>If a namespace is not provided, the namespace 'User' is used.</para>
    /// </summary>
    static member fromString (value:string) : CfVariableRef =
                            let n,ns = 
                                match value.Split([|"::"|], 2, System.StringSplitOptions.None) with
                                | [| ns ; name |] -> name,ns
                                | [| name |] -> name, "User"
                                | _ -> failwith "invalid variable name"
                            { ``namespace`` = ns; name = n }
    /// Translates a CfVariableRef to the form 'namespace::name'
    static member toString (value:CfVariableRef) : string = sprintf "%s::%s" value.``namespace`` value.name
    /// Explicit constructor for CfVariableRef
    static member create ns name = { ``namespace`` = ns ; name = name }
    /// Create a new CfVariableRef which differs only by namespace
    static member setNamespace ns vr = { vr with ``namespace`` = ns }
    /// Create a new CfVariableRef which differs only by name
    static member setName name vr = { vr with name = name }
    /// Retrieve the namespace from a CfVariableRef
    static member getNamespace vr = vr.``namespace``
    /// Retrieve the name from a CfVariableRef
    static member getName vr = vr.name

/// Definition of an SSIS variable
type CfVariable =
    {
        ``namespace`` : string
        name : string
        value: CfData
        expression : CfExpression option
        isReadOnly : bool
        raiseChangedEvent : bool
    }

/// Reference to another object in the SSIS package
[<RequireQualifiedAccessAttribute>]
type CfRef =
    | CurrentObject
//    | AbsoluteExecutableRef of string list
//    | RelativeExecutableRef of string list
    | ConnectionManagerRef of string
    | VariableRef of CfVariableRef
    static member get_current_object = function | CurrentObject -> () | _ -> failwith "Invalid reference type"
//    static member get_absolute_executable_ref = function | AbsoluteExecutableRef ref -> ref | _ -> failwith "Invalid reference type"
//    static member get_relative_executable_ref = function | RelativeExecutableRef ref -> ref | _ -> failwith "Invalid reference type"
    static member get_connection_manager_ref = function | ConnectionManagerRef ref -> ref | _ -> failwith "Invalid reference type"
    static member get_variable_ref = function | VariableRef ref -> ref | _ -> failwith "Invalid reference type"

/// Outcome of an executable execution
type CfExecutableResult =
    | Success = 0
    | Failure = 1
    | Completion = 2

/// Isolation level
type CfIsolationLevel = 
        | Serializable = 1048576
        | Snapshot = 16777216
        | RepeatableRead = 65536
        | ReadUncommitted = 1033
        | ReadCommitted = 4096
        | Chaos = 16
        | Unspecified = -1

/// Transaction option
type CfTransactionOption =
    | NotSupported = 0
    | Supported = 1
    | Required = 2

/// Precedence constraint mode
type CfPrecedenceConstraintMode =
    | Expression = 1
    | Constraint = 2
    | ExpressionAndConstraint = 3
    | ExpressionOrConstraint = 4

/// Precedence constraint definition
module _CfPrecedenceConstraintLogic =
    /// Precedence constraint definition
    type T =
        | Constraint of CfExecutableResult
        | Expression of CfExpression
        | ExpressionAndConstraint of CfExpression*CfExecutableResult
        | ExpressionOrConstraint of CfExpression*CfExecutableResult
    
        /// Translate a CfPrecedenceConstraintLogic instance to the corresponding CfPrecedenceConstraintMode enumeration value
        static member getMode = 
              function
              | Constraint _ -> CfPrecedenceConstraintMode.Constraint
              | Expression _ -> CfPrecedenceConstraintMode.Expression
              | ExpressionAndConstraint _ -> CfPrecedenceConstraintMode.ExpressionAndConstraint
              | ExpressionOrConstraint _ -> CfPrecedenceConstraintMode.ExpressionOrConstraint

        /// Construct an instance of CfPrecedenceConstraintLogic from its components
        static member buildLogic modeEnum constraintValue expressionValue = 
              match modeEnum with
              | CfPrecedenceConstraintMode.Constraint -> constraintValue |> Option.get |> Constraint 
              | CfPrecedenceConstraintMode.Expression -> expressionValue |> Option.get |> Expression
              | CfPrecedenceConstraintMode.ExpressionAndConstraint -> (expressionValue |> Option.get, constraintValue |> Option.get) |> ExpressionAndConstraint
              | CfPrecedenceConstraintMode.ExpressionOrConstraint -> (expressionValue |> Option.get, constraintValue |> Option.get) |> ExpressionOrConstraint
              | _ -> failwith "Invalid precedence constraint mode specified"

/// Precedence constraint definition
type CfPrecedenceConstraintLogic = _CfPrecedenceConstraintLogic.T

/// Precedence constraint definition
type CfPrecedenceConstraint =
        {
            description: string
            name: string

            logic: CfPrecedenceConstraintLogic
            sourceExecutableName: string
        }

/// Collection of compatible precedence constraints
module _CfPrecedenceConstraints =
    /// Collection of compatible precedence constraints
    type T =
        | Empty
        | All of CfPrecedenceConstraint list
        | Any of CfPrecedenceConstraint list

        /// Combine precedence constraints where all constraints must be satisfied
        static member addAll value = function
            | Empty -> All [value]
            | All pcs -> All (value::pcs)
            | Any pcs -> failwith "Precedence constraints must all be of the same mode"

        /// Combine precedence constraints where at least one constraint must be satisfied
        static member addAny value = function
            | Empty -> Any [value]
            | All pcs -> failwith "Precedence constraints must all be of the same mode"
            | Any pcs -> Any (value::pcs)

/// Collection of compatible precedence constraints
type CfPrecedenceConstraints = _CfPrecedenceConstraints.T

/// File access mode
type CfFileUsage =
    | UseExistingFile = 0
    | CreateFile = 1
    | UseExistingFolder = 2
    | CreateFolder = 3

/// Row format for flat files
module _CfFlatFileRowFormat = 
    /// Row format for flat files
    type T = 
        | Delimited
        | FixedWidth
        | RaggedRight
        static member toString = function
                            | Delimited -> "Delimited"
                            | FixedWidth -> "FixedWidth"
                            | RaggedRight -> "RaggedRight"
        static member fromString = function
                            | "Delimited" -> Delimited
                            | "FixedWidth" -> FixedWidth
                            | "RaggedRight" -> RaggedRight
                            | _ -> failwith "Invalid format value"

/// Row format for flat files
type CfFlatFileRowFormat = _CfFlatFileRowFormat.T

/// Column format for flat files
module _CfFlatFileColumnFormat = 
    type T = 
        | Delimited
        | FixedWidth
        /// Converts the discriminated union case to the internal SSIS textual value
        static member toString = function
                            | Delimited -> "Delimited"
                            | FixedWidth -> "FixedWidth"
        /// Interprets the internal SSIS textual value to a discriminated union case
        static member fromString = function
                            | "Delimited" -> Delimited
                            | "FixedWidth" -> FixedWidth
                            | _ -> failwith "Invalid format value"

/// Column format for flat files
type CfFlatFileColumnFormat = _CfFlatFileColumnFormat.T

/// OleDb connection manager definition
type CfOleDbConnectionManager =
    {
        description: string
        name: string
        expressions: CfPropertyExpressions

        retainConnections: bool
        connectionString: string
        delayValidation: bool
    }

/// ADO.NET connection manager definition
type CfAdoNetSqlConnectionManager =
    {
        description: string
        name: string
        expressions: CfPropertyExpressions
    
        retainConnections: bool
        connectionString: string
        delayValidation: bool
    }

/// Flat file (text rows and columns) column specification
type CfFlatFileColumn =
    {
        name: string
        format: CfFlatFileColumnFormat
        delimiter: string
        width: int
        maxWidth: int
        dataType: CfDataType
        precision: int
        scale: int
        textQualified: bool
    }

/// Flat file (text rows and columns) connection manager definition
type CfFlatFileConnectionManager =
    {
        description: string
        name: string
        expressions: CfPropertyExpressions

        connectionString: string
        delayValidation: bool
        fileUsageType: CfFileUsage
        format: CfFlatFileRowFormat
        localeId: int option
        unicode: bool
        codePage: int option
        headerRowsToSkip: int
        headerRowDelimiter: string
        columnNamesInFirstDataRow: bool
        rowDelimiter: string
        dataRowsToSkip: int
        textQualifier: string

        columns: CfFlatFileColumn list
    }

/// File (opaque object) connection manager definition
type CfFileConnectionManager = 
    {
        description: string
        name: string
        expressions: CfPropertyExpressions

        connectionString: string
        delayValidation: bool
        fileUsageType: CfFileUsage
    }

/// A connection manager definition (any supported type)
type CfConnectionManager =
    | CfFileConnectionManager of CfFileConnectionManager
    | CfFlatFileConnectionManager of CfFlatFileConnectionManager
    | CfOleDbConnectionManager of CfOleDbConnectionManager
    | CfAdoNetSqlConnectionManager of CfAdoNetSqlConnectionManager

/// SQL log provider
type CfSqlLogProvider =
    {
        name: string
        connection: CfRef
        delayValidation: bool
    }

/// Log provider
type CfLogProvider =
    | CfSqlLogProvider of CfSqlLogProvider

/// <summary>Log event filter methodology</summary>
/// <remarks>SSIS has very weird behaviour around logging. Test your settings! These settings just mirror what SSIS can support. </remarks>
type CfLogFilterKind =
    | /// Indicates that the events added to the event filter are included in the event log. 
      ByInclusion = 0
    | /// Indicates that the events added to the event filter are excluded from the event log.
      ByExclusion = 1

/// Log configuration mode
type CfLogMode =
    | UseParentSetting = 0
    | Enabled = 1
    | Disabled = 2

/// Log settings for a specific event
type CfLogEventSettings =
    {
        eventName: string
        columns: string list
    }

/// Logging options (top level logging object)
type CfLoggingOptions =
    {
        filterKind : CfLogFilterKind
        loggingMode : CfLogMode
        logProviders : string list

        logSelections : CfLogEventSettings list
    }

/// Package configuration based on a parent package variable
type CfParentVariablePackageConfiguration =
    {
        description: string
        name: string
        expressions: CfPropertyExpressions

        parentVariableName: CfVariableRef
        ignoreParentVariableNamespace: bool // used in cases where SSIS must locate a variable with an unknown namespace (bad practice but used in some cases)
        targetProperty: string
    }

/// Package configuration (of any supported type)
type CfPackageConfiguration =
    | CfParentVariablePackageConfiguration of CfParentVariablePackageConfiguration
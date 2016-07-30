namespace Chimayo.Ssis.Ast.DataFlow

open Chimayo.Ssis.Common
open Chimayo.Ssis.Ast.ControlFlow

/// A defined name
type DfNamedEntity = | DfName of string with static member decode (DfName n) = n
/// A reference to a component within the same data flow
type DfComponentReference = | DfComponentReference of string with static member decode (DfComponentReference c) = c
/// A reference to a component output within the same data flow
type DfOutputReference = 
    | DfOutputReference of DfComponentReference * DfNamedEntity
    static member build componentName outputName = (DfComponentReference componentName , DfName outputName) |> DfOutputReference
    static member decode (DfOutputReference (c,n)) = c |> DfComponentReference.decode , n |> DfNamedEntity.decode
/// A reference to an output column of a component
type DfInputColumnReference = 
    | DfInputColumnReference of DfOutputReference * DfNamedEntity
    static member build componentName outputName columnName = (DfOutputReference.build componentName outputName , DfName columnName) |> DfInputColumnReference
    static member decode (DfInputColumnReference (oref, n)) =
                        let c,o = oref |> DfOutputReference.decode
                        c , o , n |> DfNamedEntity.decode
/// A connection between a component output and a named input (attached to the input component)
type DfInputConnection = 
    | DfInputConnection of DfNamedEntity * DfOutputReference
    static member build outputComponentName outputName inputName = (DfName inputName , DfOutputReference.build outputComponentName outputName) |> DfInputConnection
    static member decode (DfInputConnection (n, oref)) =
                        let c,o = oref |> DfOutputReference.decode
                        n |> DfNamedEntity.decode, c, o


/// A data flow data type
module _DfDataType =
    type T =
        | Empty
        | Int8
        | UInt8
        | Int16
        | Int32
        | Real32
        | Real64
        | CalendarYear
        | Date
        | Boolean
        | Variant
        | Decimal of int (* scale *) // based on OLE DB decimal that has a fixed precision of 29 -- see https://connect.microsoft.com/SQLServer/feedback/details/477262/ssis-flat-file-connection-manager-cannot-set-precision-for-decimal-columns
        | UInt16
        | UInt32
        | Int64
        | UInt64
        | Guid
        | Bytes of int (* length *)
        | String of int (* code page *) * int (* length *)
        | UnicodeString of int (* length *)
        | Numeric of int (* precision *) * int (* scale *)
        | DbDate
        | DbDateTime
        | DbTime
        | DbTimeStamp
        | Image
        | Text of int (* code page *)
        | NText
        | DbTime2 of int (* scale *)
        | DbTimeStamp2 of int (* scale *)
        | DbTimeStampOffset of int (* scale *)

/// A data flow data type
type DfDataType = _DfDataType.T

/// Comparison flags defined on a column
[<System.Flags>]
type DfComparisonFlags =
    | Zero = 0
    | IgnoreCase = 0x00000001  
    | IgnoreNonspaceCharacters = 0x00000002  
    | IgnoreSymbols = 0x00000004  
    | LinguisticallyAppropriateIgnoreCase = 0x00000010  
    | LinguisticallyAppropriateIgnoreNonspaceCharacters = 0x00000020  
    | IgnoreKanaType = 0x00010000  
    | IgnoreWidth = 0x00020000  
    | UseLinguisticRulesForCasing = 0x08000000  
    
/// Special flags describing an output column
type DfOutputColumnSpecialFlags =
    | NoSpecialInformation = 0
    | ContainsErrorCodes = 1
    | ContainsLineageId = 2

/// Output column row disposition
module _DfOutputColumnRowDisposition =
    /// Output column row disposition
    type T =
        | NotUsed
        | IgnoreFailure
        | FailComponent
        | RedirectRow
        /// Converts the discriminated union case to the internal SSIS textual value
        static member toString = function
           | NotUsed -> "NotUsed"
           | IgnoreFailure -> "IgnoreFailure"
           | FailComponent -> "FailComponent"
           | RedirectRow -> "RedirectRow"
        /// Interprets the internal SSIS textual value to a discriminated union case
        static member fromString = function
           | "NotUsed" -> NotUsed
           | "IgnoreFailure" -> IgnoreFailure
           | "FailComponent" -> FailComponent
           | "RedirectRow" -> RedirectRow
           | _ -> failwith "Invalid value"

/// Output column row disposition
type DfOutputColumnRowDisposition = _DfOutputColumnRowDisposition.T

/// Specification of a Flat File Source column
type DfFlatFileSourceFileColumn = 
    {
        includeInOutput: bool

        externalName: string
        externalDataType: DfDataType

        name: string
        description: string
        dataType: DfDataType
        sortKeyPosition: int option
        comparisonFlags: DfComparisonFlags
        specialFlags: DfOutputColumnSpecialFlags

        errorRowDisposition: DfOutputColumnRowDisposition
        truncationRowDisposition: DfOutputColumnRowDisposition

        fastParse: bool
        useBinaryFormat: bool
    }

/// Code page or Unicode flag for Flat File Source component
module _DfFlatFileSourceCodePage =
    /// Code page or Unicode flag for Flat File Source component
    type T =
    | Unicode
    | CodePage of int option

/// Code page or Unicode flag for Flat File Source component
type DfFlatFileSourceCodePage = _DfFlatFileSourceCodePage.T

/// Specification of a Flat File Source component
type DfFlatFileSourceConfiguration =
    {
        retainNulls : bool
        fileNameColumnName : string
        codePage : DfFlatFileSourceCodePage

        connection: CfRef

        fileColumns : DfFlatFileSourceFileColumn list
    }

/// Specification of a Data Conversion column
type DfDataConversionColumn =
    {
        sourceColumn : DfInputColumnReference
        name : string
        dataType: DfDataType

        errorRowDisposition: DfOutputColumnRowDisposition
        truncationRowDisposition: DfOutputColumnRowDisposition
        fastParse: bool
    }

/// Specification of a Data Conversion component
type DfDataConversionConfiguration =
    {
        columns : DfDataConversionColumn list
    }


/// A sub-expression used in a data flow that may refer to columns
type DfSubExpression =
    /// A generic embedded expression string
    | Dfe of string
    /// A quoted string
    | DfeQuoted of string
    /// A reference to a data flow column
    | DfeColumnRef of DfInputColumnReference

/// A data flow expression composed by concatenating the contained sub-expressions
type DfExpression =
    | DfExpression of DfSubExpression list
    static member normalise (DfExpression e) = 
                        e
                        |> swap (List.foldBack (fun next state -> match state,next with Dfe right::rest , Dfe left -> Dfe (left+right) :: rest | _ -> next::state)) []
                        |> DfExpression

/// Derived Column column behaviour
module _DfDerivedColumnColumnBehaviour =
    /// Derived Column column behaviour
    type T =
        | ReplaceColumn of DfInputColumnReference * DfExpression
        | NewColumn of DfNamedEntity * DfDataType * DfExpression

/// Derived Column column behaviour
type DfDerivedColumnColumnBehaviour = _DfDerivedColumnColumnBehaviour.T

/// Derived Column column specification
type DfDerivedColumnColumn =
    {
        behaviour : DfDerivedColumnColumnBehaviour

        errorRowDisposition: DfOutputColumnRowDisposition
        truncationRowDisposition: DfOutputColumnRowDisposition
    }

/// Derived Column component specification
type DfDerivedColumnConfiguration =
    {
        columns: DfDerivedColumnColumn list
    }

/// Specification of an OleDb Destination fast load option
module _DfOleDbDestinationFastLoadOption =
    type T = 
        | Ordering of (string * bool (* ascending *) ) list
        | RowsPerBatch of int
        | KilobytesPerBatch of int
        | Tablock
        | CheckConstraints
        | FireTriggers

/// Specification of an OleDb Destination fast load option
type DfOleDbDestinationFastLoadOption = _DfOleDbDestinationFastLoadOption.T

/// OleDb Destination column specification
type DfOleDbDestinationColumn =
    {
        externalName: string
        externalDataType: DfDataType
        sourceColumn: DfInputColumnReference option
    }

/// OleDb Destination fast load settings
type DfOleDbDestinationFastLoadSettings =
    {
        keepIdentity : bool
        keepNulls : bool
        options : DfOleDbDestinationFastLoadOption list
        maxRowsPerCommit : int
    }

/// OleDb Destination target
module _DfOleDbDestinationTarget =
    /// OleDb Destination target
    type T =
        | TableOrView of string
        | TableOrViewVariable of CfVariableRef
        | SqlTarget of string
        | FastLoadTableOrView of string * DfOleDbDestinationFastLoadSettings
        | FastLoadTableOrViewVariable of CfVariableRef * DfOleDbDestinationFastLoadSettings

/// OleDb Destination target
type DfOleDbDestinationTarget = _DfOleDbDestinationTarget.T

/// OleDb Destination component specification
type DfOleDbDestinationConfiguration =
    {
        timeoutSeconds : int
        target : DfOleDbDestinationTarget
        alwaysUseDefaultCodePage : bool
        defaultCodePage : int
        connection : CfRef
        columns : DfOleDbDestinationColumn list
        errorRowDisposition : DfOutputColumnRowDisposition
    }

/// Parameter direction
type DfParameterDirection =
    | DfInputParameter
    | DfOutputParameter
    | DfInputOutputParameter

/// Tuple of parameter direction and a variable reference
type OleDbSourceParameterSpec = DfParameterDirection * CfVariableRef

/// OleDb Source input specification
module _DfOleDbSourceInput =
    /// OleDb Source input specification
    type T =
        | OpenRowset of string
        | OpenRowsetVariable of CfVariableRef
        | SqlCommand of string * OleDbSourceParameterSpec list
        | SqlCommandVariable of CfVariableRef * OleDbSourceParameterSpec list

/// OleDb Source input specification
type DfOleDbSourceInput = _DfOleDbSourceInput.T

/// OleDb Source column specification
type DfOleDbSourceColumn =
    {
        includeInOutput: bool

        externalName: string
        externalDataType: DfDataType

        name: string
        description: string
        dataType: DfDataType
        sortKeyPosition: int option
        comparisonFlags: DfComparisonFlags
        specialFlags: DfOutputColumnSpecialFlags

        errorRowDisposition: DfOutputColumnRowDisposition
        truncationRowDisposition: DfOutputColumnRowDisposition
    }

/// OleDb Source component specification
type DfOleDbSourceConfiguration =
    {
        connection: CfRef
        timeoutSeconds : int
        source : DfOleDbSourceInput
        alwaysUseDefaultCodePage : bool
        defaultCodePage : int
        columns : DfOleDbSourceColumn list
    }

/// Cache mode for Lookup component
module _DfLookupCacheMode =
    /// Cache mode for Lookup component
    type T =
        | None
        | Partial
        | Full

/// Cache mode for Lookup component
type DfLookupCacheMode = _DfLookupCacheMode.T

/// A reference table column in the Lookup component used for joining
type DfLookupJoinColumn =
    {
        referenceTableColumnName: string
        sourceColumn: DfInputColumnReference
        parameterIndex: int option
    }

/// A reference table column in the Lookup component added to the output
type DfLookupOutputColumn =
    {
        name: DfNamedEntity
        referenceTableColumnName: string
        dataType: DfDataType

        truncationRowDisposition: DfOutputColumnRowDisposition
    }

/// Lookup component specification
type DfLookupConfiguration =
    {
        connection: CfRef
        isCacheConnection: bool
        cacheMode: DfLookupCacheMode
        useNoMatchOutput: bool
        noMatchCachePercentage: int
        maxMemoryUsageMbX86: int
        maxMemoryUsageMbX64: int
        source: string
        parameterisedSource: string
        defaultCodePage: int
        /// <remarks>Not supported in SSIS 2008</remarks>
        treatDuplicateKeysAsErrors: bool
        
        joinColumns : DfLookupJoinColumn list
        outputColumns : DfLookupOutputColumn list

        errorRowDisposition : DfOutputColumnRowDisposition
    }

/// Recordset Destination column specification
type DfRecordsetDestinationColumn =
    {
        sourceColumn: DfInputColumnReference
        readOnly: bool
    }

/// Recordset Destination component specification
type DfRecordsetDestinationConfiguration =
    {
        variable: CfVariableRef
        columns: DfRecordsetDestinationColumn list
    }

/// Expected number of unique records
module _DfAggregateScaling =
    /// Expected number of unique records
    type T =
        | Unspecified
        | Low
        | Medium
        | High

/// Expected number of unique records
type DfAggregateScaling = _DfAggregateScaling.T

/// Expected number of unique records and an absolute value, the latter taking precedence if provided.
type DfAggregateScalingValue = DfAggregateScaling * int option

/// Comparison flags used by Aggregate component
[<System.Flags>]
type DfAggregateComparisonFlags =
    | Zero = 0
    | IgnoreCase = 0x00000001  
    | IgnoreNonspaceCharacters = 0x00000002  
    | IgnoreKanaType = 0x00010000  
    | IgnoreWidth = 0x00020000  
    
/// Aggregation operation
module _DfAggregateOperation =
    /// Aggregation operation
    type T =
        | GroupBy of DfAggregateComparisonFlags
        | Min
        | Max
        | Sum
        | Avg
        | Count
        | CountAll
        | CountDistinct of DfAggregateComparisonFlags * DfAggregateScalingValue

/// Aggregation operation
type DfAggregateOperation = _DfAggregateOperation.T

/// Configuration of a single output column
type DfAggregateColumn =
    {
        name: DfNamedEntity
        sourceColumn: DfInputColumnReference option // not used when performing Count All
        logic: DfAggregateOperation
        dataType: DfDataType
    }

/// Configuration of an output for the Aggregate component
type DfAggregateAggregation =
    {
        outputName: DfNamedEntity
        columns: DfAggregateColumn list
        keyScaling: DfAggregateScalingValue
    }

/// Aggregate component specification
type DfAggregateConfiguration =
    {
        aggregations: DfAggregateAggregation list
        keyScaling: DfAggregateScalingValue
        countDistinctScaling: DfAggregateScalingValue
        autoExtendFactor: int
    }

/// Specification of a conditional output for the Conditional Split component
type DfConditionalSplitOutput =
    {
        outputName: DfNamedEntity
        condition: DfExpression
    }

/// Conditional Split component specification
type DfConditionalSplitConfiguration =
    {
        conditionalOutputs: DfConditionalSplitOutput list
        defaultOutputName: DfNamedEntity
    }


/// Tuple of input name and source column reference used in Union All component
type DfUnionAllInputColumn =
    | DfUnionAllInputColumn of DfNamedEntity * DfInputColumnReference 

/// Specification of an output column for the Union All component
type DfUnionAllColumn =
    {
        name: DfNamedEntity
        dataType: DfDataType
        mappedInputColumns: DfUnionAllInputColumn list
    }

/// Union All component specification
type DfUnionAllConfiguration =
    {
        columns : DfUnionAllColumn list
    }

/// Row Count component specification
type DfRowCountConfiguration =
    {
        resultVariable : CfVariableRef
    }

/// Location of XML source data
module _DfXmlSourceMapping =
  /// Location of XML source data
  type T =
    | XmlFile of string
    | XmlFileVariable of CfVariableRef
    | XmlData of CfVariableRef

/// Location of XML source data
type DfXmlSourceMapping = _DfXmlSourceMapping.T

/// Location of XML schema
module _DfXmlSourceSchemaSource =
  /// Location of XML schema
  type T = 
    | InlineSchema
    | ExternalSchema of string

/// Location of XML schema
type DfXmlSourceSchemaSource = _DfXmlSourceSchemaSource.T

/// Definition of an XML Source output column
type DfXmlSourceOutputColumn =
    {
        /// Name of the column
        name: DfNamedEntity
        /// Data flow data type
        dataType: DfDataType
        /// Data flow data type for error output
        errorOutputDataType: DfDataType
        /// Underlying CLR type; note that only a subset of types are supported
        clrType: string

        /// Error behaviour
        errorRowDisposition: DfOutputColumnRowDisposition
        /// Truncation behaviour
        truncationRowDisposition: DfOutputColumnRowDisposition
    }

/// Definition of an XML Source output
type DfXmlSourceOutput =
    {
        /// Output name
        name: DfNamedEntity
        /// Defines the specific collection output by the component
        rowset: string
        /// The set of columns defined for the output
        columns: DfXmlSourceOutputColumn list
    }

/// Integer mapping mode for XML Source component
module _DfXmlSourceIntegerMode =
    /// Integer mapping mode for XML Source component
    type T =
        | Int32
        | Decimal

/// Integer mapping mode for XML Source component
type DfXmlSourceIntegerMode = _DfXmlSourceIntegerMode.T

/// Definition of the XML Source component
type DfXmlSourceConfiguration =
    {
        /// Location of the XML data
        source: DfXmlSourceMapping
        /// Location of the XML schema
        xmlSchema: DfXmlSourceSchemaSource
        /// Output definition
        outputs: DfXmlSourceOutput list
        /// Integer mapping mode
        integerMode: DfXmlSourceIntegerMode
    }

/// Component-specific configuration of any supported data flow component
type DfComponentConfiguration =
    | DfFlatFileSource of DfFlatFileSourceConfiguration
    | DfDataConversion of DfDataConversionConfiguration
    | DfDerivedColumn of DfDerivedColumnConfiguration
    | DfOleDbDestination of DfOleDbDestinationConfiguration
    | DfOleDbSource of DfOleDbSourceConfiguration
    | DfLookup of DfLookupConfiguration
    | DfRecordsetDestination of DfRecordsetDestinationConfiguration
    | DfMulticast // no specific configuration required
    | DfAggregate of DfAggregateConfiguration
    | DfConditionalSplit of DfConditionalSplitConfiguration
    | DfUnionAll of DfUnionAllConfiguration
    | DfRowCount of DfRowCountConfiguration
    | DfXmlSource of DfXmlSourceConfiguration

/// A data flow component specification
type DfComponent =
    {
        name: string
        description: string
        localeId: int option
        usesDispositions: bool option
        validateExternalMetadata: bool option

        inputConnections: DfInputConnection list
        inputs: DfNamedEntity list
        outputs: DfNamedEntity list
        outputColumns: (DfNamedEntity * DfNamedEntity * DfDataType) list

        configuration: DfComponentConfiguration
    }

/// A data flow
type DfPipeline =
    {
        components: DfComponent list
    }

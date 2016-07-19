namespace Chimayo.Ssis.Ast.DataFlowApi

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Ast
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.DataFlow

/// Functions to support working with pipeline components
module PipelineCommon =
    /// Translates a CfDataType into the equivalent DfDataType
    let translate_data_type dt codepage length precision scale =
        match dt with
        | CfDataType.Empty -> DfDataType.Empty
        | CfDataType.Null -> failwith "No conversion from Null type"
        | CfDataType.Int16 -> DfDataType.Int16
        | CfDataType.Int32 -> DfDataType.Int32
        | CfDataType.Real32 -> DfDataType.Real32
        | CfDataType.Real64 -> DfDataType.Real64
        | CfDataType.Currency -> DfDataType.Decimal (scale)
        | CfDataType.Date -> DfDataType.Date
        | CfDataType.String -> DfDataType.String (codepage, length)
        | CfDataType.Boolean -> DfDataType.Boolean
        | CfDataType.Object -> failwith "No conversion from Object type"
        | CfDataType.Decimal -> DfDataType.Decimal (scale)
        | CfDataType.Int8 -> DfDataType.Int8
        | CfDataType.UInt8 -> DfDataType.UInt8
        | CfDataType.UInt16 -> DfDataType.UInt16
        | CfDataType.UInt32 -> DfDataType.UInt32
        | CfDataType.Int64 -> DfDataType.Int64
        | CfDataType.UInt64 -> DfDataType.UInt64
        | CfDataType.IntNoSize -> failwith "No conversion from IntNoSize type"
        | CfDataType.UIntNoSize -> failwith "No conversion from UIntNoSize type"
        | CfDataType.Timestamp64 -> failwith "No conversion from Timestamp64 type"
        | CfDataType.Guid -> DfDataType.Guid
        | CfDataType.VarString -> DfDataType.String (codepage, length)
        | CfDataType.NullTerminatedUnicodeString -> DfDataType.UnicodeString length
        | CfDataType.Numeric -> DfDataType.Numeric (precision, scale)
        | CfDataType.DbDate -> DfDataType.DbDate
        | CfDataType.DbTime -> DfDataType.DbTime
        | CfDataType.DbTimeStamp  -> DfDataType.DbTimeStamp
        | CfDataType.Varnumeric -> failwith "No conversion from Varnumeric type"
        | CfDataType.DbTimeExtended -> failwith "No conversion from DbTimeExtended type"
        | CfDataType.DbTimeStampOffset -> DfDataType.DbTimeStampOffset scale
        | _ -> failwith "Invalid data type"

    /// An empty DfPipeline instance
    let empty : DfPipeline = { components = [] }

    /// Set the name of a pipeline component
    let set_name value (c:DfComponent) = { c with name = value }
    /// Set the descripion of a pipeline component
    let set_description value (c:DfComponent) = { c with description = value }
    /// Set the locale id of a pipeline component
    let set_locale_id value (c:DfComponent) = { c with localeId = value }
    /// Set the uses dispositions flag of a pipeline component
    let set_uses_dispositions value (c:DfComponent) = { c with usesDispositions = value }
    /// Set the validate external metadata flag of a pipeline component
    let set_validate_external_metadata value (c:DfComponent) = { c with validateExternalMetadata = value }

    /// Get the name of a pipeline component
    let get_name (c:DfComponent) = c.name
    /// Get the description of a pipeline component
    let get_description (c:DfComponent) = c.description
    /// Get the locale id of a pipeline component
    let get_locale_id (c:DfComponent) = c.localeId
    /// Get the uses dispositions flag of a pipeline component
    let get_uses_dispositions (c:DfComponent) = c.usesDispositions
    /// Get the validate external metadata flag of a pipeline component
    let get_validate_external_metadata (c:DfComponent) = c.validateExternalMetadata

    /// Get the input connections of a pipeline component
    let get_input_connections (c:DfComponent) = c.inputConnections
    /// Get the inputs of a pipeline component
    let get_inputs (c:DfComponent) = c.inputs
    /// Get the outputs of a pipeline component
    let get_outputs (c:DfComponent) = c.outputs

    /// Gets the pipeline components of a DfPipeline instance
    let inline get_components model = model.components

    /// Compare the name of a pipeline component to a provided value
    let inline component_name_is_equal name = get_name >> stringCompareInvariantIgnoreCase name

    /// Build a DfComponentReference from a pipeline component
    let component_ref ``component`` = ``component`` |> get_name |> DfComponentReference
    /// Build a DfOutputReference from a pipeline component and the name of an output
    let output_ref ``component`` outputName = DfOutputReference (component_ref ``component``, DfName outputName)
    /// Build a DfInputColumnReference from a pipeline component, the name of an output and the name of the output column
    let input_column_ref ``component`` outputName columnName = DfInputColumnReference (output_ref ``component`` outputName, DfName columnName)

    /// Extract the components of a DfInputConnection into a tuple of input name, pipeline component name and output name
    let decode_input_connection = function
        | DfInputConnection (DfName inputName, DfOutputReference (DfComponentReference cname, DfName outputName)) -> inputName, cname, outputName

    /// Extract the components of a DfInputColumnReference into a tuple of pipeline component name, output name and column name
    let decode_input_column_ref = function
        | DfInputColumnReference (DfOutputReference (DfComponentReference cname, DfName oname), DfName colName) -> cname, oname, colName

    /// Adds a pipeline component to a DfPipeline instance, checking for name conflicts
    let add_component ``component`` model = 
        let adder c m = { m with components = c :: m.components }
        let tryAdd' = tryAdd get_components get_name adder stringCompareInvariantIgnoreCase
        tryAdd' ``component`` model

    /// Adds multiple pipeline components to a DfPipeline instance, checking for name conflicts
    let add_components<'a> = swapAndFoldList add_component

    /// Applies a mapping function to a single pipeline component within a DfPipeline instance, identified by pipeline component name
    let mutate_component fn name model =
        { model with components = model.components |> listMapIf fn (component_name_is_equal name) }

    /// Replaces a pipeline component within a DfPipeline instance by matching pipeline component name.  No changes occur if the pipeline component name is not matched.
    let update_component c = mutate_component (defer c) c.name

    /// Updates a pipeline component with new configuration data
    let internal update_component_data data c = { c with configuration = data }

    /// Translates an instance of DfInputColumnReference to a tuple of output name, output column name and output column data type
    let deref_input_column_ref sourceColumnRef model =
        let cname, oname, colname = decode_input_column_ref sourceColumnRef
        model.components 
        |> listFindL (fun c -> stringCompareInvariantIgnoreCase c.name cname) (fun () -> sprintf "deref_input_column_ref: component not found (%A)" sourceColumnRef)
        |> fun c -> c.outputColumns
        |> listFindL (fun (DfName oname',DfName colName', _) -> (stringCompareInvariantIgnoreCase oname oname') &&  (stringCompareInvariantIgnoreCase colname colName') )
                     (fun () -> sprintf "deref_input_column_ref: output column not found (%A)" sourceColumnRef)

    /// Gets the data type associated with a DfInputColumnReference
    let get_input_column_ref_data_type sourceColumnRef model = deref_input_column_ref sourceColumnRef model |> fun (_,_,dt) -> dt

    /// Valdates that a pipeline component has an output with the provided name
    let has_named_output outputName c = c.outputs |> List.map (function DfName name -> name) |> Set.ofList |> Set.contains outputName

    /// Connects two pipeline components
    let internal connect_input_by_name inputName sourceComponentName outputName c =
        let defined_inputs = c |> get_inputs |> List.map DfNamedEntity.decode |> Set.ofList
        (defined_inputs |> Set.contains inputName |> not) @?! "Input is not defined for this component"
        let connected_inputs = c |> get_input_connections |> List.map (DfInputConnection.decode >> fun (i,_,_) -> i) |> Set.ofList
        (connected_inputs |> Set.contains inputName) @?! "Input is already connected"
        { c with DfComponent.inputConnections = DfInputConnection (DfName inputName, DfOutputReference.build sourceComponentName outputName) :: c.inputConnections }

    /// Connects two pipeline components
    let internal connect_input inputName sourceComponent outputName =
        connect_input_by_name inputName (sourceComponent |> get_name) outputName

    /// Removes the connection to the specified input, if any
    let internal disconnect_input inputName c =
        { c with inputConnections = c.inputConnections |> List.filter (DfInputConnection.decode >> fun (i,_,_) -> stringCompareInvariant inputName i |> not ) }

    /// Removes all input connections
    let internal discounnt_all_inputs c =
        c 
        |> (get_input_connections >> List.map (DfInputConnection.decode >> fun (x,_,_) -> x))
        |> List.fold (swap disconnect_input) c


/// API for creating and manipulating Flat File Source pipeline components
module FlatFileSource =
    open PipelineCommon

    /// Extracts configuration data
    let private ingress = function | { configuration = DfFlatFileSource typedData } -> typedData | _ -> failwith "Invalid data type"
    /// Wraps configuration data in a DfComponentConfiguration
    let private egress = DfFlatFileSource
    /// Performs a mapping on configuration data
    let private mutate fn c = update_component_data (c |> ingress |> fn |> egress) c

    /// Alias for ingress
    let get = ingress
        
    /// Defined name of the flat file source output. Has to match SSIS internals.
    let output_name = "Flat File Source Output"
    /// Defined name of the flat file source error output. Has to match SSIS internals.
    let error_output_name = "Flat File Source Error Output"

    /// Default code page for non-Unicode data
    let default_code_page = System.Text.Encoding.Default.WindowsCodePage
    /// Defined name of the flat file source error output column. Has to match SSIS internals.
    let error_output_column_name = @"Flat File Source Error Output Column"

    /// Initial output columns
    let private initial_output_columns =
        [ 
            DfName error_output_name , DfName @"ErrorCode"  , DfDataType.Int32
            DfName error_output_name , DfName @"ErrorColumn"  , DfDataType.Int32
            DfName error_output_name , DfName error_output_column_name  , DfDataType.Text default_code_page 
        ]

    /// Create a new instance of a Flat File Source component and then apply a series of transformations
    let create name adapters =
        adapters
        |> List.fold reverseInvoke
            {
                name = name
                description = ""
                localeId = None
                usesDispositions = Some true
                validateExternalMetadata = Some true

                inputConnections = [ ]
                inputs = []
                outputs = [ DfName output_name ; DfName error_output_name ]
                outputColumns = initial_output_columns

                configuration =
                    DfFlatFileSource
                        {
                            retainNulls = true
                            fileNameColumnName = ""
                            codePage = DfFlatFileSourceCodePage.CodePage (Some default_code_page)
                            connection = CfRef.CurrentObject
                            fileColumns = []
                        }
            }

    /// Get the retain NULLs flag
    let get_retain_nulls = ingress >> fun d -> d.retainNulls
    /// Get the name of the filename column
    let get_filename_column_name = ingress >> fun d -> d.fileNameColumnName
    /// Get the connection
    let get_connection = ingress >> fun d -> d.connection
    /// Get the code page for non-Unicode data
    let get_code_page = ingress >> fun d -> d.codePage

    /// Ensures the error output column properly matches the definition of the Flat File Source
    let private update_error_output_column c =
        let codePage = c |> get_code_page
        let dataType = 
            codePage 
            |> function 
               | DfFlatFileSourceCodePage.CodePage cp -> cp |> optionOrDefault default_code_page |> DfDataType.Text
               | _ -> DfDataType.NText
        let mapIf test fn = List.map <| fun value -> value |> (if test value then fn else id)
        let isErrorColumn (DfName oname,DfName cname,_) = (stringCompareInvariant oname error_output_name) && (stringCompareInvariant cname error_output_column_name)
        let newErrorColumn = DfName error_output_name, DfName error_output_column_name, dataType
        {
            c with outputColumns = c.outputColumns |> mapIf isErrorColumn (defer newErrorColumn)
        }

    /// Set the retain NULLs flag
    let set_retain_nulls enabled = mutate (fun d -> { d with retainNulls = enabled })
    /// Set the name of the filename column
    let set_filename_column_name columnName = mutate (fun d -> { d with fileNameColumnName = columnName })
    /// Set the connection
    let set_connection conn = mutate (fun d -> { d with connection = conn })
    /// Set the code page for non-Unicode data
    let set_code_page codePage = mutate (fun d -> { d with codePage = codePage }) >> update_error_output_column

    /// Adds a column to the list of exposed output columns of the pipeline component
    let private add_output_column (column : DfFlatFileSourceFileColumn) (c : DfComponent) =
        { c with outputColumns = (DfName output_name, DfName column.name, column.dataType) :: c.outputColumns }
    /// Adds multiple column to the list of exposed output columns of the pipeline component
    let private add_output_columns = swapAndFoldList add_output_column

    /// Clear all output columns
    let clear_columns =
        mutate (fun d -> { d with fileColumns = [] })
        >> fun c -> { c with outputColumns = initial_output_columns }
        >> update_error_output_column

    /// Adds a column to the pipeline component
    let add_column (column : DfFlatFileSourceFileColumn) = 
        mutate (fun d -> { d with fileColumns = column :: d.fileColumns }) >> (column.includeInOutput |> (add_output_column column) @?@ id)
    /// Adds multiple columns to the pipeline component
    let add_columns = swapAndFoldList add_column

    /// Get the columns defined for the pipeline component
    let get_columns = ingress >> fun d -> d.fileColumns

    /// Construct a new Flat File Source column definition with values set to appropriate defaults
    let define_column name dataType =
        {
            includeInOutput = true
            externalName = name
            externalDataType = dataType

            name = name
            description = ""
            dataType = dataType
            sortKeyPosition = None
            comparisonFlags = DfComparisonFlags.Zero
            specialFlags = DfOutputColumnSpecialFlags.NoSpecialInformation

            errorRowDisposition = DfOutputColumnRowDisposition.FailComponent
            truncationRowDisposition = DfOutputColumnRowDisposition.FailComponent

            fastParse = false
            useBinaryFormat = false
        }

    /// Link a Flat File Source component to a connection manager and reset code page and columns to match
    let link_connection (cm : CfConnectionManager) =
        let cm' = cm |> function | CfFlatFileConnectionManager ffcm -> ffcm | _ -> failwith "Can only link to a flat file connection manager"
        let columnCodePage = cm'.codePage |> optionOrDefault System.Text.Encoding.Default.WindowsCodePage
        let generate_column (c : CfFlatFileColumn) =
            {
                define_column c.name (translate_data_type c.dataType columnCodePage c.maxWidth c.precision c.scale)
                with
                    includeInOutput = true
            }
        let columns = cm'.columns |> List.map generate_column
        set_connection (CfRef.ConnectionManagerRef cm'.name)
        >> clear_columns
        >> add_columns columns
        >> set_code_page (cm'.unicode |> DfFlatFileSourceCodePage.Unicode @?@ DfFlatFileSourceCodePage.CodePage cm'.codePage)
        
/// API for creating and manipulating Data Conversion pipeline components
module DataConversion =
    open PipelineCommon

    /// Extracts configuration data
    let private ingress = function | { configuration = DfDataConversion typedData } -> typedData | _ -> failwith "Invalid data type"
    /// Wraps configuration data in a DfComponentConfiguration
    let private egress = DfDataConversion
    /// Performs a mapping on configuration data
    let private mutate fn c = update_component_data (c |> ingress |> fn |> egress) c

    /// Alias for ingress
    let get = ingress

    /// Defined name of the Data Conversion output. Has to match SSIS internals.
    let output_name = "Data Conversion Output"
    /// Defined name of the Data Conversion error output. Has to match SSIS internals.
    let error_output_name = "Data Conversion Error Output"
    /// Defined name of the Data Conversion input. Has to match SSIS internals.
    let input_name = "Data Conversion Input"

    /// Initial output columns
    let private initial_output_columns =
        [ (DfName error_output_name , DfName "ErrorCode", DfDataType.Int32) ; (DfName error_output_name , DfName "ErrorColumn", DfDataType.Int32) ]

    /// Create a new instance of a Data Conversion component and then apply a series of transformations
    let create name adapters : DfComponent =
        adapters
        |> List.fold reverseInvoke
            {
                name = name
                description = ""
                localeId = None
                usesDispositions = Some true
                validateExternalMetadata = None

                inputConnections = [ ]
                inputs = [ DfName input_name ]
                outputs = [ DfName error_output_name ; DfName output_name ]
                outputColumns = initial_output_columns

                configuration = DfDataConversion { columns = [] }
            }
    
    /// Clear all output columns
    let clear_columns =
        fun (c:DfComponent) -> { c with outputColumns = initial_output_columns }

    /// Add a column to the pipeline component
    let add_column (column : DfDataConversionColumn) c = 
        let c' = mutate (fun d -> { d with columns = column :: d.columns }) c
        { c' with outputColumns = (DfName output_name, DfName column.name, column.dataType) :: c'.outputColumns }
    /// Add multiple columns to the pipeline component
    let add_columns = swapAndFoldList add_column
    /// Get the columns of the pipeline component
    let get_columns = ingress >> fun d -> d.columns

    /// Construct a new Data Conversion column definition with values set to appropriate defaults
    let define_column name dataType sourceColumn =
        {
            DfDataConversionColumn.name = name
            dataType = dataType
            sourceColumn = sourceColumn
            errorRowDisposition = DfOutputColumnRowDisposition.FailComponent
            truncationRowDisposition = DfOutputColumnRowDisposition.FailComponent
            fastParse = false
        }

    /// Connects two pipeline components
    let connect_input = PipelineCommon.connect_input input_name
    /// Connects two pipeline components
    let connect_input_by_name = PipelineCommon.connect_input_by_name input_name
    /// Removes the connection, if any
    let disconnect_input = PipelineCommon.disconnect_input input_name

/// API for creating and manipiulating Derived Column pipeline components
module DerivedColumn =
    open PipelineCommon

    /// Extracts configuration data
    let private ingress = function | { configuration = DfDerivedColumn typedData } -> typedData | _ -> failwith "Invalid data type"
    /// Wraps configuration data in a DfComponentConfiguration
    let private egress = DfDerivedColumn
    /// Performs a mapping on configuration data
    let private mutate fn c = update_component_data (c |> ingress |> fn |> egress) c

    /// Alias for ingress
    let get = ingress

    /// Defined name of the Derived Column output. Has to match SSIS internals.
    let output_name = "Derived Column Output"
    /// Defined name of the Derived Column error output. Has to match SSIS internals.
    let error_output_name = "Derived Column Error Output"
    /// Defined name of the Derived Column input. Has to match SSIS internals.
    let input_name = "Derived Column Input"

    /// Initial output columns
    let private initial_output_columns =
        [ (DfName error_output_name , DfName "ErrorCode", DfDataType.Int32) ; (DfName error_output_name , DfName "ErrorColumn", DfDataType.Int32) ]

    /// Create a new instance of a Derived Column component and then apply a series of transformations
    let create name adapters : DfComponent =
        adapters
        |> List.fold reverseInvoke
            {
                name = name
                description = ""
                localeId = None
                usesDispositions = Some true
                validateExternalMetadata = None

                inputConnections = [ ]
                inputs = [ DfName input_name ]
                outputs = [ DfName error_output_name ; DfName output_name ]
                outputColumns = initial_output_columns

                configuration = DfDerivedColumn { columns = [] }
            }

    /// Add a column to the pipeline component
    let add_column (column : DfDerivedColumnColumn) c = 
        let c' = mutate (fun d -> { d with columns = column :: d.columns }) c
        let newOutputColumns = 
            match column.behaviour with
            | DfDerivedColumnColumnBehaviour.NewColumn (name,dt,_) -> (DfName output_name, name, dt) :: c'.outputColumns
            | _ -> c'.outputColumns
        { c' with outputColumns = newOutputColumns }
    /// Add multiple columns to the pipeline component
    let add_columns = swapAndFoldList add_column
    /// Get the columns of the pipeline component
    let get_columns = ingress >> fun d -> d.columns

    /// Clear all output columns
    let clear_columns =
        mutate (fun d -> { d with columns = [] })
        >> fun c -> { c with outputColumns = initial_output_columns }

    /// Constructs a new Derived Column column that produces a new output column
    let define_column name dataType expression =
        {
            DfDerivedColumnColumn.behaviour = DfDerivedColumnColumnBehaviour.NewColumn (DfName name , dataType , expression)
            errorRowDisposition = DfOutputColumnRowDisposition.FailComponent
            truncationRowDisposition = DfOutputColumnRowDisposition.FailComponent
        }
    /// Constructs a new Derived Column column that replaces the value of an input column
    let replace_column sourceColumn expression =
        {
            DfDerivedColumnColumn.behaviour = DfDerivedColumnColumnBehaviour.ReplaceColumn (sourceColumn, expression)
            errorRowDisposition = DfOutputColumnRowDisposition.FailComponent
            truncationRowDisposition = DfOutputColumnRowDisposition.FailComponent
        }

    /// Connects two pipeline components
    let connect_input = PipelineCommon.connect_input input_name
    /// Connects two pipeline components
    let connect_input_by_name = PipelineCommon.connect_input_by_name input_name
    /// Removes the connection, if any
    let disconnect_input = PipelineCommon.disconnect_input input_name
    
/// API for creating and manipiulating OLE DB Destination pipeline components
module OleDbDestination =
    open PipelineCommon

    /// Extracts configuration data
    let private ingress = function | { configuration = DfOleDbDestination typedData } -> typedData | _ -> failwith "Invalid data type"
    /// Wraps configuration data in a DfComponentConfiguration
    let private egress = DfOleDbDestination
    /// Performs a mapping on configuration data
    let private mutate fn c = update_component_data (c |> ingress |> fn |> egress) c

    /// Alias for ingress
    let get = ingress

    /// Defined name of the OLE DB Destination error output. Has to match SSIS internals.
    let error_output_name = "OLE DB Destination Error Output"
    /// Defined name of the OLE DB Destination input. Has to match SSIS internals.
    let input_name = "OLE DB Destination Input"

    /// Create a new instance of an OLE DB Destination component and then apply a series of transformations
    let create name adapters : DfComponent =
        adapters
        |> List.fold reverseInvoke
            {
                name = name
                description = ""
                localeId = None
                usesDispositions = Some true
                validateExternalMetadata = None

                inputConnections = [ ]
                inputs = [ DfName input_name ]
                outputs = [ DfName error_output_name ]
                outputColumns = [ (DfName error_output_name , DfName "ErrorCode", DfDataType.Int32) ; (DfName error_output_name , DfName "ErrorColumn", DfDataType.Int32) ]

                configuration = DfOleDbDestination 
                    { 
                        DfOleDbDestinationConfiguration.timeoutSeconds = 0
                        target = DfOleDbDestinationTarget.TableOrView ""
                        alwaysUseDefaultCodePage = false
                        defaultCodePage = System.Text.Encoding.Default.WindowsCodePage
                        columns = []
                        connection = CfRef.ConnectionManagerRef ""
                        errorRowDisposition = DfOutputColumnRowDisposition.FailComponent
                    }
            }

    /// Construct an instance of DfOleDbDestinationFastLoadSettings
    let build_fast_load_options keepIdentity keepNulls maxRowsPerCommit options =
        {
            DfOleDbDestinationFastLoadSettings.keepIdentity = keepIdentity
            keepNulls = keepNulls
            options = options
            maxRowsPerCommit = maxRowsPerCommit
        }

    /// Set the target of the pipeline component directly
    let set_target t = mutate <| fun d -> { d with target = t }
    /// Retargets the pipelne component to populate a table or view by name
    let set_target_table_or_view = DfOleDbDestinationTarget.TableOrView >> set_target
    /// Retargets the pipelne component to populate a table or view by name stored in a variable
    let set_target_table_or_view_variable = DfOleDbDestinationTarget.TableOrViewVariable >> set_target
    /// Retargets the pipelne component to populate a table or view represented by a SQL statement
    let set_target_sql = DfOleDbDestinationTarget.SqlTarget >> set_target
    /// Retargets the pipelne component to populate a table or view by name with fast load options
    let set_target_table_or_view_fast_load name keepIdentity keepNulls maxRowsPerCommit options = 
        let flo = build_fast_load_options keepIdentity keepNulls maxRowsPerCommit options 
        (name,flo) |> DfOleDbDestinationTarget.FastLoadTableOrView |> set_target 
    /// Retargets the pipelne component to populate a table or view by name stored in a variable with fast load options
    let set_target_table_or_view_variable_fast_load variable keepIdentity keepNulls maxRowsPerCommit options = 
        let flo = build_fast_load_options keepIdentity keepNulls maxRowsPerCommit options 
        (variable,flo) |> DfOleDbDestinationTarget.FastLoadTableOrViewVariable |> set_target 

    /// Set the timeout
    let set_timeout_seconds timeout = mutate <| fun d -> { d with timeoutSeconds = timeout }
    /// Set the always use default code page flag
    let set_always_use_default_codepage enabled = mutate <| fun d -> { d with alwaysUseDefaultCodePage = enabled }
    /// Set the default code page
    let set_default_codepage codePage = mutate <| fun d -> { d with defaultCodePage = codePage }
    /// Set the error row disposition
    let set_error_row_disposition disposition = mutate <| fun d -> { d with errorRowDisposition = disposition }
    /// Set the connection
    let set_connection conn = mutate <| fun d -> { d with connection = CfRef.ConnectionManagerRef conn }

    /// Get the timeout
    let get_timeout_seconds = ingress >> fun d -> d.timeoutSeconds
    /// Get the always use default code page flag
    let get_always_use_default_codepage = ingress >> fun d -> d.alwaysUseDefaultCodePage
    /// Get the default code page
    let get_default_codepage = ingress >> fun d -> d.defaultCodePage
    /// Get the error row disposition
    let get_error_row_disposition = ingress >> fun d -> d.errorRowDisposition
    /// Get the target
    let get_target = ingress >> fun d -> d.target
    /// Get the connection
    let get_connection = ingress >> fun d -> d.connection

    /// Add a column
    let add_column (column : DfOleDbDestinationColumn) c = mutate (fun d -> { d with columns = column :: d.columns }) c
    /// Add multiple columns
    let add_columns = swapAndFoldList add_column
    /// Get the columns
    let get_columns = ingress >> fun d -> d.columns
    /// Removes all columns
    let clear_columns = mutate (fun d -> { d with columns = [] })
    /// Removes a column based on its external name
    let remove_column_by_external_name externalName = mutate (fun d -> { d with columns = d.columns |> List.filter (fun col -> stringCompareInvariant externalName col.externalName |> not) })
    /// Removes all columns
    [<System.Obsolete("Prefer clear_columns")>]
    let remove_all_columns = clear_columns

    /// Construct a new OLE DB Destination column definition
    let define_column externalName externalDataType sourceColumnRef =
        {
            DfOleDbDestinationColumn.externalName = externalName
            externalDataType = externalDataType
            sourceColumn = sourceColumnRef
        }

    /// Construct multiple OLE DB Destination column definitions from a list of tuples of (externalName, externalDataType, sourceColumnName)
    let define_columns_for_source componentName outputName =
        List.map <| fun (externalName,externalDataType,sourceColumnName) -> 
                        define_column externalName externalDataType
                                      (sourceColumnName |> DfInputColumnReference.build componentName outputName |> Some)

    /// Connects two pipeline components
    let connect_input = PipelineCommon.connect_input input_name
    /// Connects two pipeline components
    let connect_input_by_name = PipelineCommon.connect_input_by_name input_name
    /// Removes the connection, if any
    let disconnect_input = PipelineCommon.disconnect_input input_name

    /// Gets a column based on its external name
    let get_column_by_external_name externalName = get_columns >> List.find (fun c -> c.externalName |> stringCompareInvariant externalName)

/// API for creating and manipulating OLE DB Source pipeline components
module OleDbSource =
    open PipelineCommon

    /// Extracts configuration data
    let private ingress = function | { configuration = DfOleDbSource typedData } -> typedData | _ -> failwith "Invalid data type"
    /// Wraps configuration data in a DfComponentConfiguration
    let private egress = DfOleDbSource
    /// Performs a mapping on configuration data
    let private mutate fn c = update_component_data (c |> ingress |> fn |> egress) c

    /// Alias for ingress
    let get = ingress

    /// Defined name of the OLE DB Source error output. Has to match SSIS internals.
    let error_output_name = "OLE DB Source Error Output"
    /// Defined name of the OLE DB Source output. Has to match SSIS internals.
    let output_name = "OLE DB Source Output"

    /// Initial output columns
    let private initial_output_columns =
        [ 
            (DfName error_output_name, DfName "ErrorCode", DfDataType.Int32) 
            (DfName error_output_name, DfName "ErrorColumn", DfDataType.Int32) 
        ]

    /// Create a new instance of an OLE DB Source component and then apply a series of transformations
    let create name adapters : DfComponent =
        adapters
        |> List.fold reverseInvoke
            {
                name = name
                description = ""
                localeId = None
                usesDispositions = Some true
                validateExternalMetadata = None

                inputConnections = [ ]
                inputs = [ ]
                outputs = [ DfName output_name ; DfName error_output_name ]
                outputColumns = initial_output_columns

                configuration = DfOleDbSource 
                    { 
                        DfOleDbSourceConfiguration.timeoutSeconds = 0
                        source = DfOleDbSourceInput.OpenRowset ""
                        alwaysUseDefaultCodePage = false
                        defaultCodePage = System.Text.Encoding.Default.WindowsCodePage
                        columns = []
                        connection = CfRef.ConnectionManagerRef ""
                    }
            }

    /// Set timeout
    let set_timeout_seconds timeout = mutate <| fun d -> { d with timeoutSeconds = timeout }
    /// Set always use default code page flag
    let set_always_use_default_codepage enabled = mutate <| fun d -> { d with alwaysUseDefaultCodePage = enabled }
    /// Set default code page
    let set_default_codepage codePage = mutate <| fun d -> { d with defaultCodePage = codePage }
    /// Set the connection
    let set_connection conn = mutate <| fun d -> { d with connection = CfRef.ConnectionManagerRef conn }
    /// Set the source specification
    let set_source source = mutate <| fun d -> { d with source = source }

    /// Get the timeout
    let get_timeout_seconds = ingress >> fun d -> d.timeoutSeconds
    /// Get the always use default code page flag
    let get_always_use_default_codepage = ingress >> fun d -> d.alwaysUseDefaultCodePage
    /// Get the default code page
    let get_default_codepage = ingress >> fun d -> d.defaultCodePage
    /// Get the source specification
    let get_source = ingress >> fun d -> d.source
    /// Get the connection
    let get_connection = ingress >> fun d -> d.connection

    /// Adds a column to the list of exposed output columns of the pipeline component
    let private add_output_column (column : DfOleDbSourceColumn) (c : DfComponent) =
        match column with
        | { includeInOutput = true } ->
            { c with outputColumns = 
                        (DfName output_name, DfName column.name, column.dataType) 
                        :: (DfName error_output_name, DfName column.name, column.externalDataType) 
                        :: c.outputColumns }
        | _ -> c
    /// Adds multiple column to the list of exposed output columns of the pipeline component
    let private add_output_columns = swapAndFoldList add_output_column

    /// Clear all output columns
    let clear_columns = 
        mutate (fun d -> { d with columns = [] })
        >> fun c -> { c with outputColumns = initial_output_columns }

    /// Adds a column to the pipeline component
    let add_column (column : DfOleDbSourceColumn) = 
        mutate (fun d -> { d with columns = column :: d.columns }) >> add_output_column column
    /// Adds multiple columns to the pipeline component
    let add_columns = swapAndFoldList add_column
    /// Get the columns defined for the pipeline component
    let get_columns = ingress >> fun d -> d.columns

    /// Construct a new OLE DB Source column with values set to appropriate defaults
    let define_column name dataType =
        {
            includeInOutput = true
            
            externalName = name
            externalDataType = dataType

            name = name
            description = ""
            dataType = dataType
            sortKeyPosition = None
            comparisonFlags = DfComparisonFlags.Zero
            specialFlags = DfOutputColumnSpecialFlags.NoSpecialInformation

            errorRowDisposition = DfOutputColumnRowDisposition.FailComponent
            truncationRowDisposition = DfOutputColumnRowDisposition.FailComponent
        }

/// API for creating and manipulating Lookup pipeline components
module Lookup =
    open PipelineCommon

    /// Extracts configuration data
    let private ingress = function | { configuration = DfLookup typedData } -> typedData | _ -> failwith "Invalid data type"
    /// Wraps configuration data in a DfComponentConfiguration
    let private egress = DfLookup
    /// Performs a mapping on configuration data
    let private mutate fn c = update_component_data (c |> ingress |> fn |> egress) c

    /// Alias for ingress
    let get = ingress


    /// Defined name of the Lookup error output. Has to match SSIS internals.
    let error_output_name = "Lookup Error Output"
    /// Defined name of the Lookup output. Has to match SSIS internals.
    let output_name = "Lookup Match Output"
    /// Defined name of the Lookup input. Has to match SSIS internals.
    let input_name = "Lookup Input"
    /// Defined name of the Lookup no match output. Has to match SSIS internals.
    let no_match_output_name = "Lookup No Match Output"

    /// Initial output columns
    let private initial_output_columns = 
        [
            DfName error_output_name , DfName @"ErrorCode"  , DfDataType.Int32
            DfName error_output_name , DfName @"ErrorColumn"  , DfDataType.Int32
        ]

    /// Create a new instance of a Lookup component and then apply a series of transformations
    let create name adapters : DfComponent =
        adapters
        |> List.fold reverseInvoke
            {
                name = name
                description = ""
                localeId = None
                usesDispositions = Some true
                validateExternalMetadata = None

                inputConnections = [ ]
                inputs = [ DfName input_name ]
                outputs = [ DfName output_name ; DfName no_match_output_name ; DfName error_output_name ]
                outputColumns = initial_output_columns
                    

                configuration = DfLookup
                    { 
                        connection = CfRef.ConnectionManagerRef ""
                        isCacheConnection = false
                        cacheMode = DfLookupCacheMode.Full
                        useNoMatchOutput = false
                        noMatchCachePercentage = 0
                        maxMemoryUsageMbX86 = 25
                        maxMemoryUsageMbX64 = 25
                        source = ""
                        parameterisedSource = ""
                        defaultCodePage = System.Text.Encoding.Default.WindowsCodePage
                        treatDuplicateKeysAsErrors = true
        
                        joinColumns  = []
                        outputColumns  = []

                        errorRowDisposition = DfOutputColumnRowDisposition.FailComponent
                    }
            }

    /// Get the status of the no match output
    let has_no_match_ouput = ingress >> fun l -> l.useNoMatchOutput

    /// Add a join column
    let add_join_column column = mutate (fun l -> { l with joinColumns = column::l.joinColumns })
    /// Add multiple join columns
    let add_join_columns = swapAndFoldList add_join_column

    /// Adds a column to the list of exposed output columns of the pipeline component
    let add_output_column (column:DfLookupOutputColumn) c = 
        c
        |> mutate (fun l -> { l with outputColumns = column::l.outputColumns })
        |> fun c' -> { c' with outputColumns = (DfName output_name , column.name , column.dataType ) :: c'.outputColumns }
    /// Adds multiple column to the list of exposed output columns of the pipeline component
    let add_output_columns = swapAndFoldList add_output_column

    /// Remove all join columns
    let clear_join_columns = mutate <| fun l -> { l with joinColumns = [] }
    /// Remove all output columns
    let clear_output_columns = 
        mutate <| fun l -> { l with outputColumns = [] } 
        >> fun c -> {c with outputColumns = initial_output_columns }
    /// Remove all defined columns
    let clear_columns = clear_join_columns >> clear_output_columns

    /// Change the value of the has_no_match_output flag
    let private reset_columns has_no_match_output c =
        let config = c |> ingress
        let join, output = config.joinColumns, config.outputColumns
        c
        |> clear_columns
        |> ( mutate <| fun l -> { l with useNoMatchOutput = true } )
        |> add_join_columns join
        |> add_output_columns output

    /// Enable the no match output
    let enable_no_match_output = reset_columns true
    /// Disable the no match output
    let disable_no_match_output = reset_columns false

    /// Determines if the cache connection is enabled
    let get_cache_connection_enabled = ingress >> fun l -> l.isCacheConnection
    /// Get the cache mode
    let get_cache_mode = ingress >> fun l -> l.cacheMode
    /// Get the connection
    let get_connection = ingress >> fun l -> l.connection
    /// Get the no match cache percentage
    let get_no_match_cache_percentage = ingress >> fun l -> l.noMatchCachePercentage
    /// Get all join columns
    let get_join_columns = ingress >> fun l -> l.joinColumns
    /// Get all output columns
    let get_output_columns = ingress >> fun l -> l.outputColumns
    /// Get the memory limit for 32-bit execution
    let get_max_memory_usage_mb_x86 = ingress >> fun l -> l.maxMemoryUsageMbX86
    /// Get the memory limit for 64-bit execution
    let get_max_memory_usage_mb_x64 = ingress >> fun l -> l.maxMemoryUsageMbX64
    /// Get the lookup data source specification
    let get_source = ingress >> fun l -> l.source
    /// Get the lookup data parameterised source specification, used when not in the full cache mode
    let get_parameterised_source = ingress >> fun l -> l.parameterisedSource
    /// Get the default code page
    let get_default_code_page = ingress >> fun l -> l.defaultCodePage
    /// Get the error row disposition
    let get_error_row_disposition = ingress >> fun l -> l.errorRowDisposition
    /// Get the treat duplicate keys as errors flag
    let get_treat_duplicate_keys_as_errors = ingress >> fun l -> l.treatDuplicateKeysAsErrors

    /// Set the cache connection enabled flag
    let set_cache_connection_enabled flag = mutate <| fun l -> { l with isCacheConnection = flag }
    /// Set the cache mode
    let set_cache_mode mode = mutate <| fun l -> { l with cacheMode = mode }
    /// Set the connection
    let set_connection conn = mutate <| fun l -> { l with connection = conn }
    /// Set the no match cache percentage
    let set_no_match_cache_percentage value = mutate <| fun l -> { l with noMatchCachePercentage = value }
    /// Replace all join columns
    let set_join_coumns columns = clear_join_columns >> add_join_columns columns
    /// Replace all output columns
    let set_output_coumns columns = clear_output_columns >> add_output_columns columns
    /// Set the memory limit for 32-bit execution
    let set_max_memory_usage_mb_x86 value = mutate <| fun l -> { l with maxMemoryUsageMbX86 = value }
    /// Set the memory limit for 64-bit execution
    let set_max_memory_usage_mb_x64 value = mutate <| fun l -> { l with maxMemoryUsageMbX64 = value }
    /// Set the lookup data source specification
    let set_source value = mutate <| fun l -> { l with source = value }
    /// Set the lookup data parameterised source specification, used when not in the full cache mode
    let set_parameterised_source value = mutate <| fun l -> { l with parameterisedSource = value }
    /// Set the default code page
    let set_default_code_page codePage = mutate <| fun l -> { l with defaultCodePage = codePage }
    /// Set the error row disposition
    let set_error_row_disposition value = mutate <| fun l -> { l with errorRowDisposition = value }
    /// Set the treat duplicate keys as errors flag
    let set_treat_duplicate_keys_as_errors value = mutate <| fun l -> { l with treatDuplicateKeysAsErrors = value }

    /// Connects two pipeline components
    let connect_input = PipelineCommon.connect_input input_name
    /// Connects two pipeline components
    let connect_input_by_name = PipelineCommon.connect_input_by_name input_name
    /// Removes the connection, if any
    let disconnect_input = PipelineCommon.disconnect_input input_name

    /// Construct a new join column definition
    let define_join_column sourceComponent sourceOutput sourceColumn referenceTableColumnName parameterIndexOption =
        {
            sourceColumn = DfInputColumnReference.build sourceComponent sourceOutput sourceColumn
            referenceTableColumnName = referenceTableColumnName
            parameterIndex = parameterIndexOption 
        } : DfLookupJoinColumn

    /// Construct a new output column definition
    let define_output_column name referenceTableColumnName dataType truncationDisposition =
        {
            referenceTableColumnName = referenceTableColumnName
            name = DfName name
            dataType = dataType
            truncationRowDisposition = truncationDisposition
        } : DfLookupOutputColumn

    /// Change the name of an output column
    let rename_output_column columnName newColumnName c =
        c
        |> clear_output_columns
        |> add_output_columns
            (
              c 
              |> get_output_columns 
              |> listMapIf ( fun column -> { column with name = DfName newColumnName } )
                           ( fun column -> column.name |> DfNamedEntity.decode |> stringCompareInvariant columnName )
            )

    /// Relink an existing join column to a different upstream column
    let remap_join_column referenceTableColumnName sourceComponent sourceOutput sourceColumn c =
        c
        |> clear_join_columns
        |> add_join_columns
            (
              c 
              |> get_join_columns 
              |> listMapIf ( fun column -> { column with sourceColumn = DfInputColumnReference.build sourceComponent sourceOutput sourceColumn } )
                           ( fun column -> stringCompareInvariant column.referenceTableColumnName referenceTableColumnName)
            )

/// API for creating and manipulating Recordset Destination pipeline components
module RecordsetDestination =
    open PipelineCommon

    /// Extracts configuration data
    let private ingress = function | { configuration = DfRecordsetDestination typedData } -> typedData | _ -> failwith "Invalid data type"
    /// Wraps configuration data in a DfComponentConfiguration
    let private egress = DfRecordsetDestination
    /// Performs a mapping on configuration data
    let private mutate fn c = update_component_data (c |> ingress |> fn |> egress) c
    /// Alias for ingress
    let get = ingress
    
    /// Defined name of the Recordset Destination input. Has to match SSIS internals.
    let input_name = "Recordset Destination Input"

    /// Create a new instance of a Recordset Destination pipeline component and then apply a series of transformations
    let create name adapters : DfComponent =
        adapters
        |> List.fold reverseInvoke
            {
                name = name
                description = ""
                localeId = None
                usesDispositions = Some false
                validateExternalMetadata = None

                inputConnections = []
                inputs = [ DfName input_name ]
                outputs = []
                outputColumns = []

                configuration = DfRecordsetDestination 
                    { 
                        DfRecordsetDestinationConfiguration.variable = CfVariableRef.fromString ""
                        columns = []
                    }
            }

    /// Set the target variable
    let set_variable value = mutate <| fun d -> { d with variable = value }
    /// Get the target variable
    let get_variable = ingress >> fun d -> d.variable
    /// Remove all columns
    let clear_columns = mutate <| fun d -> {d with columns = [] }
    /// Add a column
    let add_column column = mutate <| fun d -> { d with columns = d.columns @ [column] } // preserve order
    /// Add multiple columns
    let add_columns columns = mutate <| fun d -> { d with columns = d.columns @ columns } // preserve order
    /// Get the columns
    let get_columns = ingress >> fun d -> d.columns

    /// Construct a new column definition
    let define_column sourceColumnRef readOnly =
        {
            DfRecordsetDestinationColumn.sourceColumn = sourceColumnRef
            readOnly = readOnly
        }

    /// Connects two pipeline components
    let connect_input = PipelineCommon.connect_input input_name
    /// Connects two pipeline components
    let connect_input_by_name = PipelineCommon.connect_input_by_name input_name
    /// Removes the connection, if any
    let disconnect_input = PipelineCommon.disconnect_input input_name

/// API for creating and manipulating Multicast pipeline components
module Multicast =
    open PipelineCommon
    
    /// Extracts configuration data
    let private ingress = function | { configuration = DfMulticast } -> () | _ -> failwith "Invalid data type"
    /// Wraps configuration data in a DfComponentConfiguration
    let private egress = fun () -> DfMulticast
    /// Performs a mapping on configuration data
    let private mutate fn c = update_component_data (c |> ingress |> fn |> egress) c
    /// Alias for ingress
    let get = ingress
    
    /// Defined name of the Multicast input. Has to match SSIS internals.
    let input_name = "Multicast Input 1"

    /// Create a new instance of a Multicast pipeline component and then apply a series of transformations
    let create name adapters : DfComponent =
        adapters
        |> List.fold reverseInvoke
            {
                name = name
                description = ""
                localeId = None
                usesDispositions = Some false
                validateExternalMetadata = None

                inputConnections = []
                inputs = [ DfName input_name ]
                outputs = []
                outputColumns = []

                configuration = DfMulticast
            }

    /// Remove all outputs
    let clear_outputs c =
        do c |> ingress // asserts type
        { c with outputs = [] }
    /// Add a new output
    let add_output name c =
        do c |> ingress // asserts type
        let currentOutputs = c.outputs |> List.map (function DfName outputName -> outputName) |> Set.ofList
        if currentOutputs |> Set.contains name then
            failwith "Output name already in use"
        else
        { c with outputs = (DfName name)::c.outputs }
    /// Add multiple outputs
    let add_outputs = swapAndFoldList add_output
    /// Rename an existing output
    let rename_output originalName newName c =
        do c |> ingress // asserts type
        let currentOutputs = c.outputs |> List.map (function DfName outputName -> outputName) |> Set.ofList
        if currentOutputs |> Set.contains originalName |> not then
            failwith "Output not found"
        else
        { c with outputs = c.outputs |> List.map (function DfName outputName when stringCompareInvariant outputName originalName -> DfName newName | x -> x) }
        
    /// Connects two pipeline components
    let connect_input = PipelineCommon.connect_input input_name
    /// Connects two pipeline components
    let connect_input_by_name = PipelineCommon.connect_input_by_name input_name
    /// Removes the connection, if any
    let disconnect_input = PipelineCommon.disconnect_input input_name

/// API for creating and manipulating Aggregate pipeline components
module Aggregate =
    open PipelineCommon

    /// Extracts configuration data
    let private ingress = function | { configuration = DfAggregate typedData } -> typedData | _ -> failwith "Invalid data type"
    /// Wraps configuration data in a DfComponentConfiguration
    let private egress = DfAggregate
    /// Performs a mapping on configuration data
    let private mutate fn c = update_component_data (c |> ingress |> fn |> egress) c
    /// Alias for ingress
    let get = ingress

    /// Defined name of the Aggregate input. Has to match SSIS internals.
    let input_name = "Aggregate Input 1"

    /// Create a new instance of an Aggregate pipeline component and then apply a series of transformations
    let create name adapters : DfComponent =
        adapters
        |> List.fold reverseInvoke
            {
                name = name
                description = ""
                localeId = None
                usesDispositions = Some false
                validateExternalMetadata = None

                inputConnections = []
                inputs = [ DfName input_name ]
                outputs = []
                outputColumns = []

                configuration = DfAggregate
                    { 
                        DfAggregateConfiguration.aggregations = []
                        keyScaling = DfAggregateScaling.Unspecified , None
                        countDistinctScaling = DfAggregateScaling.Unspecified , None
                        autoExtendFactor = 25
                    }
            }
    /// Add an aggregation to the pipeline component
    let add_aggregation (value:DfAggregateAggregation) c =
        let getName (DfName name) = name
        let outputName = value.outputName |> getName
        let currentOutputs = c.outputs |> List.map getName |> Set.ofList
        if currentOutputs |> Set.contains (value.outputName |> getName) then
            failwith "Output name already in use"
        else
        { 
            c with 
                outputs = value.outputName::c.outputs
                outputColumns = c.outputColumns |> List.append (value.columns |> List.map (fun column -> value.outputName, column.name , column.dataType))
        }
        |> mutate (fun ac -> { ac with aggregations = value::ac.aggregations })

    /// Add multiple aggregations to the pipeline component
    let add_aggregations = swapAndFoldList add_aggregation
    /// Remove all aggregations from the pipeline component
    let clear_aggregations = mutate (fun a -> { a with aggregations = [] }) >> fun c -> {c with outputs = [] ; outputColumns = [] }
    /// Get the aggregations
    let get_aggregations = ingress >> fun a -> a.aggregations

    /// Get the Key scaling
    let get_key_scaling = ingress >> fun a -> a.keyScaling
    /// Get the Count Distinct scaling
    let get_count_distinct_scaling = ingress >> fun a -> a.countDistinctScaling
    /// Get the auto extend factor
    let get_auto_extend_factor = ingress >> fun a -> a.autoExtendFactor
    
    /// Set the Key scaling
    let set_key_scaling value = mutate <| fun a -> { a with keyScaling = value }
    /// Set the Count Distinct scaling
    let set_count_distinct_scaling value = mutate <| fun a -> { a with countDistinctScaling = value }
    /// Set the auto extend factor
    let set_auto_extend_factor value = mutate <| fun a -> { a with autoExtendFactor = value }

    /// Connects two pipeline components
    let connect_input = PipelineCommon.connect_input input_name
    /// Connects two pipeline components
    let connect_input_by_name = PipelineCommon.connect_input_by_name input_name
    /// Removes the connection, if any
    let disconnect_input = PipelineCommon.disconnect_input input_name

    /// Construct a new aggregation output
    let define_aggregation outputName keyScaling columns =
        {
            outputName = DfName outputName
            keyScaling = keyScaling
            columns = columns
        } : DfAggregateAggregation

    /// Construct a new aggregation output column that operates over a source column
    let define_mapped_column name dataType sourceComponent sourceOutput sourceColumn logic =
        {
            name = DfName name
            dataType = dataType
            sourceColumn = DfInputColumnReference.build sourceComponent sourceOutput sourceColumn |> Some
            logic = logic
        } : DfAggregateColumn

    /// Construct a new aggregation output column that operates without a source column (i.e. count all)
    let define_unmapped_column name dataType logic =
        {
            name = DfName name
            dataType = dataType
            sourceColumn = None
            logic = logic
        } : DfAggregateColumn

/// API for creating and manipulating Conditional Split pipeline components
module ConditionalSplit =
    open PipelineCommon

    /// Extracts configuration data
    let private ingress = function | { configuration = DfConditionalSplit typedData } -> typedData | _ -> failwith "Invalid data type"
    /// Wraps configuration data in a DfComponentConfiguration
    let private egress = DfConditionalSplit
    /// Performs a mapping on configuration data
    let private mutate fn c = update_component_data (c |> ingress |> fn |> egress) c
    /// Alias for ingress
    let get = ingress

    /// Defined name of the Conditional Split input. Has to match SSIS internals.
    let input_name = "Conditional Split Input"
    /// Default/initial name of the Conditional Split default output.
    let default_output_name = "Conditional Split Default Output"
    /// Defined name of the Conditional Split error output. Has to match SSIS internals.
    let error_output_name = "Conditional Split Error Output"

    /// Create a new instance of a Conditional Split pipeline component and then apply a series of transformations
    let create name adapters : DfComponent =
        adapters
        |> List.fold reverseInvoke
            {
                name = name
                description = ""
                localeId = None
                usesDispositions = Some true
                validateExternalMetadata = None

                inputConnections = []
                inputs = [ DfName input_name ]
                outputs = [ DfName default_output_name ; DfName error_output_name ]
                outputColumns = 
                    [
                        DfName error_output_name , DfName @"ErrorCode"  , DfDataType.Int32
                        DfName error_output_name , DfName @"ErrorColumn"  , DfDataType.Int32
                    ]

                configuration = DfConditionalSplit
                    { 
                        DfConditionalSplitConfiguration.conditionalOutputs = []
                        defaultOutputName = DfName default_output_name
                    }
            }

    /// Get the name of the default output
    let get_default_output = ingress >> fun cs -> cs.defaultOutputName |> DfNamedEntity.decode
    /// Set the name of the default output
    let set_default_output name c =
        let current_default_output_name = c |> get_default_output
        if current_default_output_name = name then c
        else
        if c |> has_named_output name then
            failwith "Output name already in use"
        else
            { c with outputs = c.outputs |> List.map (fun o -> (o = DfName current_default_output_name) |> cond (DfName name) o) }
            |> mutate (fun cs -> { cs with defaultOutputName = DfName name })

    /// Add a conditional output
    let add_conditional_output (value:DfConditionalSplitOutput) c =
        if c |> has_named_output (value.outputName |> DfNamedEntity.decode) then
            failwith "Output name already in use"
        else
        { c with outputs = value.outputName::c.outputs }
        |> mutate (fun cs -> {cs with conditionalOutputs = value::cs.conditionalOutputs })
    /// Add multiple conditional outputs
    let add_conditional_outputs = swapAndFoldList add_conditional_output
    /// Clear all conditional outputs
    let clear_conditional_outputs c = 
        let default_output = c |> get_default_output
        {c with outputs = [ DfName default_output ; DfName error_output_name ] }
        |> mutate (fun cs -> { cs with conditionalOutputs = [] })
    /// Get the condditional outputs
    let get_conditional_outputs = ingress >> fun a -> a.conditionalOutputs

    /// Connects two pipeline components
    let connect_input = PipelineCommon.connect_input input_name
    /// Connects two pipeline components
    let connect_input_by_name = PipelineCommon.connect_input_by_name input_name
    /// Removes the connection, if any
    let disconnect_input = PipelineCommon.disconnect_input input_name

    /// Construct a new conditional output definition
    let define_output outputName condition =
        {
            outputName = DfName outputName
            condition = DfExpression condition
        } : DfConditionalSplitOutput

/// API for creating and manipulating Union All pipeline components
module UnionAll =
    open PipelineCommon
    
    /// Extracts configuration data
    let private ingress = function | { configuration = DfUnionAll typedData } -> typedData | _ -> failwith "Invalid data type"
    /// Wraps configuration data in a DfComponentConfiguration
    let private egress = DfUnionAll
    /// Performs a mapping on configuration data
    let private mutate fn c = update_component_data (c |> ingress |> fn |> egress) c
    /// Alias for ingress
    let get = ingress

    /// String printer for deriving input names from an index
    let input_name_printer = sprintf "Union All Input %d"
    /// Defined name of the Union All output. Has to match SSIS internals.
    let output_name = "Union All Output 1"

    /// Create a new instance of a Union All pipeline component and then apply a series of transformations
    let create name adapters : DfComponent =
        adapters
        |> List.fold reverseInvoke
            {
                name = name
                description = ""
                localeId = None
                usesDispositions = Some false
                validateExternalMetadata = None

                inputConnections = []
                inputs = []
                outputs = [ DfName output_name ]
                outputColumns = []

                configuration = DfUnionAll
                    { 
                        DfUnionAllConfiguration.columns = []
                    }
            }

    /// Get the output columns
    let get_columns = ingress >> fun ua -> ua.columns

    /// Ensures that the component inputs and output columns match the specification of columns
    let ensure_inputs_and_outputs c =
        let cols = c |> get_columns
        let inputs = cols |> List.collect (fun x -> x.mappedInputColumns) |> List.map (fun (DfUnionAllInputColumn (iname,_)) -> iname) |> Set.ofList |> Set.toList
        { c with
                inputs = inputs
                outputColumns = cols |> List.map (fun col -> DfName output_name, col.name, col.dataType)
        }

    /// Removes all columns
    let clear_columns c = c |> mutate (fun ua -> { ua with columns = [] }) |> ensure_inputs_and_outputs

    /// Add a column to the internal configuration but do not invoke ensure_inputs_and_outputs
    let private add_column_internal (column:DfUnionAllColumn) c = c |> mutate (fun ua -> { ua with columns = column::ua.columns} )
    /// Add a column
    let add_column column = add_column_internal column >> ensure_inputs_and_outputs
    /// Add mmultiple columns
    let add_columns columns = swapAndFoldList add_column_internal columns >> ensure_inputs_and_outputs

    /// Connects two pipeline components
    let connect_input inputName = PipelineCommon.connect_input inputName
    /// Connects two pipeline components
    let connect_input_by_name inputName = PipelineCommon.connect_input_by_name inputName
    /// Removes the connection, if any
    let disconnect_input inputName = PipelineCommon.disconnect_input inputName
    /// Removes all input connections
    let disconnect_all_inputs = PipelineCommon.discounnt_all_inputs

    /// Construct a new output column definition
    let define_column columnName dataType sourceColumnMappings =
        let fn (inputName,colref) = DfUnionAllInputColumn ( DfName inputName , colref )
        {
            name = DfName columnName
            dataType = dataType
            mappedInputColumns = sourceColumnMappings |> List.map fn
        } : DfUnionAllColumn

    /// Construct a new output column definition using the same source column reference for multiple inputs
    let define_common_columns inputs columnsAndMappings =
        let build (colref, dataType) =
            let _,_,name = colref |> DfInputColumnReference.decode
            { name = DfName name ; dataType = dataType ; mappedInputColumns = inputs |> List.map (fun i -> DfUnionAllInputColumn (DfName i , colref)) }
        columnsAndMappings
        |> List.map build

/// API for creating and manipulating Row Count pipeline components
module RowCount =
    open PipelineCommon

    /// Extracts configuration data
    let private ingress = function | { configuration = DfRowCount typedData } -> typedData | _ -> failwith "Invalid data type"
    /// Wraps configuration data in a DfComponentConfiguration
    let private egress = DfRowCount
    /// Performs a mapping on configuration data
    let private mutate fn c = update_component_data (c |> ingress |> fn |> egress) c
    /// Alias for ingress
    let get = ingress

    /// Defined name of the Row Count input. Has to match SSIS internals.
    let input_name = "Row Count Input 1"
    /// Defined name of the Row Count output. Has to match SSIS internals.
    let output_name = "Row Count Output 1"

    /// Create a new instance of a Row Count pipeline component and then apply a series of transformations
    let create name adapters : DfComponent =
        adapters
        |> List.fold reverseInvoke
            {
                name = name
                description = ""
                localeId = None
                usesDispositions = Some false
                validateExternalMetadata = None

                inputConnections = []
                inputs = [ DfName input_name ]
                outputs = [ DfName output_name ]
                outputColumns = []

                configuration = DfRowCount
                    { 
                        DfRowCountConfiguration.resultVariable = CfVariableRef.fromString ""
                    }
            }

    /// Connects two pipeline components
    let connect_input = PipelineCommon.connect_input input_name
    /// Connects two pipeline components
    let connect_input_by_name = PipelineCommon.connect_input_by_name input_name
    /// Removes the connection, if any
    let disconnect_input = PipelineCommon.disconnect_input input_name

    /// Get the variable to receive the row count result
    let get_result_variable = ingress >> fun d -> d.resultVariable
    /// Set the variable to receive the row count result
    let set_result_variable v = mutate <| fun d -> { d with resultVariable = v }

/// API for creating and manipulating XML Source components
module XmlSource =
    open PipelineCommon

    /// Extracts configuration data
    let private ingress = function | { configuration = DfXmlSource typedData } -> typedData | _ -> failwith "Invalid data type"
    /// Wraps configuration data in a DfComponentConfiguration
    let private egress = DfXmlSource
    /// Performs a mapping on configuration data
    let private mutate fn c = update_component_data (c |> ingress |> fn |> egress) c
    /// Alias for ingress
    let get = ingress

    /// String printer for deriving output names from a rowset ID
    let error_output_name_printer = sprintf "%s Error Output"

    /// Create a new instance of an XML Source pipeline component and then apply a series of transformations
    let create name adapters : DfComponent =
        adapters
        |> List.fold reverseInvoke
            {
                name = name
                description = ""
                localeId = None
                usesDispositions = Some true
                validateExternalMetadata = None

                inputConnections = []
                inputs = []
                outputs = []
                outputColumns = []

                configuration = DfXmlSource
                    { 
                        DfXmlSourceConfiguration.outputs = []
                        source = DfXmlSourceMapping.XmlFile ""
                        xmlSchema = DfXmlSourceSchemaSource.ExternalSchema ""
                        integerMode = DfXmlSourceIntegerMode.Int32
                    }
            }

    /// Get the outputs defined on this XML Source component
    let get_outputs = ingress >> fun x -> x.outputs
    /// Get the XML source location
    let get_source = ingress >> fun x -> x.source
    /// Get the XML schema location
    let get_xml_schema = ingress >> fun x -> x.xmlSchema
    /// Get the integer mapping mode
    let get_integer_mode = ingress >> fun x -> x.integerMode

    /// Set the XML source location
    let set_source source = mutate <| fun x -> { x with source = source }
    /// Set the XML schema location
    let set_xml_schema xmlSchema = mutate <| fun x -> { x with xmlSchema = xmlSchema }
    /// Set the integer mappping mode
    let set_integer_mode integerMode = mutate <| fun x -> { x with integerMode = integerMode }


    /// Update the DfComponent configuration to match the set of outputs and columns defined
    /// in this XML Source component
    let private update_component_outputs c =
        let outputs = c |> get_outputs
        let errorOutputMapper outputName =
            outputName |> DfNamedEntity.decode |> error_output_name_printer |> DfName
        let outputs' = 
            outputs |> List.collect  (fun o ->  [ o.name ; o.name |> errorOutputMapper ] )
        let columnF = 
            List.collect (fun o -> o.columns |> List.map (fun xc -> o.name, xc.name, xc.dataType))
        let errorColumnF =
            List.collect 
              (fun o -> 
                  o.columns 
                  |> List.map (fun xc -> o.name |> errorOutputMapper, xc.name, xc.errorOutputDataType)
              )
        let errorCodeColumnF =
            List.collect
              (fun (o:DfXmlSourceOutput) ->
                  [ 
                    o.name |> errorOutputMapper , DfName @"ErrorCode" , DfDataType.Int32
                    o.name |> errorOutputMapper , DfName @"ErrorColumn" , DfDataType.Int32
                  ]
              )
        let outputColumns' =
            (outputs |> columnF) @ (outputs |> errorColumnF) @ (outputs |> errorCodeColumnF)
        { 
          c with outputs = outputs'
                 outputColumns = outputColumns' 
        }

    /// Remove all output definitions
    let clear_outputs =
        mutate (fun x -> { x with outputs = [] })
        >> update_component_outputs

    /// Define a new XML Source output
    let define_output name rowset columns =
        {
            name = DfName name
            rowset = rowset
            columns = columns
        }

    /// Adds an output to an XML Source configuration without updating DfComponent level data
    let private add_output_internal output = mutate (fun x -> { x with outputs = output::x.outputs })
    /// Adds an output to an XML Source component
    let add_output output = add_output_internal output >> update_component_outputs
    /// Adds multiple outputs to an XML Source component
    let add_outputs outputs = swapAndFoldList add_output_internal outputs >> update_component_outputs

    /// Define a column for an XML Source output
    let define_column name dataType clrType errorOutputDataType errorDisposition truncationDisposition =
        {
            name = DfName name
            dataType = dataType
            errorOutputDataType = errorOutputDataType
            clrType = clrType
            errorRowDisposition = errorDisposition
            truncationRowDisposition = truncationDisposition
        }

namespace Chimayo.Ssis.Ast.ControlFlowApi

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators
open Chimayo.Ssis.Ast.ControlFlow

/// API to work with common aspects of connection managers
module ConnectionManager =

    /// Get the connection manager name from a connection manager reference
    let getNameFromReference = function
        | CfRef.ConnectionManagerRef r -> r
        | _ -> failwith "Invalid connection reference"

    /// Get the name of a connection manager definition
    let getName = function
        | CfFileConnectionManager fcm -> fcm.name
        | CfFlatFileConnectionManager ffcm -> ffcm.name
        | CfOleDbConnectionManager ocm -> ocm.name
        | CfAdoNetSqlConnectionManager acm -> acm.name

    /// Transform a connection manager definition by changing the name
    let setName name = function
        | CfFileConnectionManager fcm -> CfFileConnectionManager { fcm with name = name }
        | CfFlatFileConnectionManager ffcm -> CfFlatFileConnectionManager { ffcm with name = name }
        | CfOleDbConnectionManager ocm -> CfOleDbConnectionManager { ocm with name = name }
        | CfAdoNetSqlConnectionManager acm -> CfAdoNetSqlConnectionManager { acm with name = name }

    /// Get the description of a connection manager definition
    let getDescription = function
        | CfFileConnectionManager fcm -> fcm.description
        | CfFlatFileConnectionManager ffcm -> ffcm.description
        | CfOleDbConnectionManager ocm -> ocm.description
        | CfAdoNetSqlConnectionManager acm -> acm.description

    /// Transform a connection manager definition by changing the description
    let setDescription description = function
        | CfFileConnectionManager fcm -> CfFileConnectionManager { fcm with description = description }
        | CfFlatFileConnectionManager ffcm -> CfFlatFileConnectionManager { ffcm with description = description }
        | CfOleDbConnectionManager ocm -> CfOleDbConnectionManager { ocm with description = description }
        | CfAdoNetSqlConnectionManager acm -> CfAdoNetSqlConnectionManager { acm with description = description }

    /// Get the property expressions of a connection manager definition
    let getExpressions = function
        | CfFileConnectionManager fcm -> fcm.expressions
        | CfFlatFileConnectionManager ffcm -> ffcm.expressions
        | CfOleDbConnectionManager ocm -> ocm.expressions
        | CfAdoNetSqlConnectionManager acm -> acm.expressions

    /// Transform a connection manager definition by clearing all property expressions
    let clearExpressions = function
        | CfFileConnectionManager fcm -> CfFileConnectionManager { fcm with expressions = [] }
        | CfFlatFileConnectionManager ffcm -> CfFlatFileConnectionManager { ffcm with expressions = [] }
        | CfOleDbConnectionManager ocm -> CfOleDbConnectionManager { ocm with expressions = [] }
        | CfAdoNetSqlConnectionManager acm -> CfAdoNetSqlConnectionManager { acm with expressions = [] }

    /// Transform a connection manager definition by adding a property expression
    let addExpression targetProperty expr = function
        | CfFileConnectionManager fcm -> CfFileConnectionManager { fcm with expressions = fcm.expressions |> Expressions.addPropertyExpressionDirect targetProperty expr }
        | CfFlatFileConnectionManager ffcm -> CfFlatFileConnectionManager { ffcm with expressions = ffcm.expressions |> Expressions.addPropertyExpressionDirect targetProperty expr }
        | CfOleDbConnectionManager ocm -> CfOleDbConnectionManager { ocm with expressions = ocm.expressions |> Expressions.addPropertyExpressionDirect targetProperty expr }
        | CfAdoNetSqlConnectionManager acm -> CfAdoNetSqlConnectionManager { acm with expressions = acm.expressions |> Expressions.addPropertyExpressionDirect targetProperty expr }

/// API to work with OLE DB connection managers
module OleDbConnectionManager =
    /// Assert that the provided connection manager definition is for an OLE DB connection manager
    let private assertType cm = match cm with CfOleDbConnectionManager _ -> cm | _ -> failwith "Not an OleDbConnectionManager"
    /// Get the connection manager definition
    let private ingress cm = match cm with CfOleDbConnectionManager c -> c | _ -> failwith "Not an OleDbConnectionManager"
    /// Wrap the connection manager definition in a CfConnectionManager case
    let private egress cm = CfOleDbConnectionManager cm
    /// Transform an OLE DB connection manager definition
    let private mutate fn = ingress >> fn >> egress

    /// Create a new instance of an OLE DB connection manager definition
    let create name connectionString =
        {
            CfOleDbConnectionManager.description = ""
            name = name
            expressions = []

            retainConnections = false
            connectionString = connectionString
            delayValidation = true
        }
        |> egress

    /// Transform an OLE DB connection manager definition by adding a property expression
    let addExpression targetProperty expr = assertType >> ConnectionManager.addExpression targetProperty expr 

    /// Transform an OLE DB connection manager definition by adding an expression for a SQL Server connection string
    let addSqlServerConnectionStringExpression (svr:#obj) (dbName:#obj) upOption (appName:#obj)=
        let cs = SqlConnectionStringHelper.createOleDbConnectionStringExpression svr dbName upOption appName
        addExpression "ConnectionString" cs

    /// Get the delay validation flag
    let getDelayValidation = ingress >> fun cm -> cm.delayValidation
    /// Set the delay validation flag
    let setDelayValidation delayValidation = mutate <| fun cm -> { cm with delayValidation = delayValidation }

    /// Get the connection string
    let getConnectionString = ingress >> fun cm -> cm.connectionString
    /// Set the connection string
    let setConnectionString connectionString = mutate <| fun cm -> { cm with connectionString = connectionString }

    /// Get the retain connections flag
    let getRetainConnections = ingress >> fun cm -> cm.retainConnections
    /// Set the retain connections flag
    let setRetainConnections retainConnections = mutate <| fun cm -> { cm with retainConnections = retainConnections }

/// API to work with ADO.NET connection managers
module AdoNetSqlConnectionManager =
    /// Assert that the provided connection manager definition is for an ADO.NET connection manager
    let private assertType cm = match cm with CfAdoNetSqlConnectionManager _ -> cm | _ -> failwith "Not an AdoNetSqlConnectionManager"
    /// Get the connection manager definition
    let private ingress cm = match cm with CfAdoNetSqlConnectionManager c -> c | _ -> failwith "Not an AdoNetSqlConnectionManager"
    /// Wrap the connection manager definition in a CfConnectionManager case
    let private egress cm = CfAdoNetSqlConnectionManager cm
    /// Transform an ADO.NET connection manager definition
    let private mutate fn = ingress >> fn >> egress

    /// Create a new instance of an ADO.NET connection manager definition
    let create name connectionString =
        {
            CfAdoNetSqlConnectionManager.description = ""
            name = name
            expressions = []

            retainConnections = false
            connectionString = connectionString
            delayValidation = true
        }
        |> egress

    /// Transform an ADO.NET connection manager definition by adding a property expression
    let addExpression targetProperty expr = assertType >> ConnectionManager.addExpression targetProperty expr 

    /// Transform an ADO.NET connection manager definition by adding an expression for a SQL Server connection string
    let addSqlServerConnectionStringExpression (svr:#obj) (dbName:#obj) upOption (appName:#obj)=
        let cs = SqlConnectionStringHelper.createAdoNetConnectionStringExpression svr dbName upOption appName
        addExpression "ConnectionString" cs

    /// Get the delay validation flag
    let getDelayValidation = ingress >> fun cm -> cm.delayValidation
    /// Set the delay validation flag
    let setDelayValidation delayValidation = mutate <| fun cm -> { cm with delayValidation = delayValidation }

    /// Get the connection string
    let getConnectionString = ingress >> fun cm -> cm.connectionString
    /// Set the connection string
    let setConnectionString connectionString = mutate <| fun cm -> { cm with connectionString = connectionString }

    /// Get the retain connections flag
    let getRetainConnections = ingress >> fun cm -> cm.retainConnections
    /// Set the retain connections flag
    let setRetainConnections retainConnections = mutate <| fun cm -> { cm with retainConnections = retainConnections }

/// API to work with File connection managers
module FileConnectionManager =
    /// Assert that the provided connection manager definition is for a file connection manager
    let private assertType cm = match cm with CfFileConnectionManager _ -> cm | _ -> failwith "Not a FileConnectionManager"
    /// Get the connection manager definition
    let private ingress cm = match cm with CfFileConnectionManager c -> c | _ -> failwith "Not a FileConnectionManager"
    /// Wrap the connection manager definition in a CfConnectionManager case
    let private egress cm = CfFileConnectionManager cm
    /// Transform a file connection manager definition
    let private mutate fn = ingress >> fn >> egress

    /// Create a new instance of a file connection manager definition
    let create name fileUsageType connectionString =
        {
            CfFileConnectionManager.description = ""
            name = name
            expressions = []

            connectionString = connectionString
            delayValidation = false
            fileUsageType = fileUsageType
        }
        |> egress

    /// Transform a file connection manager definition by adding a property expression
    let addExpression targetProperty expr = assertType >> ConnectionManager.addExpression targetProperty expr 

    /// Transform a file connection manager definition by adding a property expression for the connection string property
    let addConnectionStringExpression expr = addExpression "ConnectionString" expr
        
    /// Get the file usage type
    let getFileUsageType = ingress >> fun cm -> cm.fileUsageType
    /// Set the file usage type
    let setFileUsageType fileUsageType = mutate <| fun cm -> { cm with fileUsageType = fileUsageType }
    
    /// Get the delay validation flag
    let getDelayValidation = ingress >> fun cm -> cm.delayValidation
    /// Set the delay validation flag
    let setDelayValidation delayValidation = mutate <| fun cm -> { cm with delayValidation = delayValidation }

    /// Get the connection string
    let getConnectionString = ingress >> fun cm -> cm.connectionString
    /// Set the connection string
    let setConnectionString connectionString = mutate <| fun cm -> { cm with connectionString = connectionString }

/// API to work with Flat File connection managers
module FlatFileConnectionManager =
    /// Assert that the provided connection manager definition is for a file connection manager
    let private assertType cm = match cm with CfFlatFileConnectionManager _ -> cm | _ -> failwith "Not a FileConnectionManager"
    /// Get the connection manager definition
    let private ingress cm = match cm with CfFlatFileConnectionManager c -> c | _ -> failwith "Not a FileConnectionManager"
    /// Wrap the connection manager definition in a CfConnectionManager case
    let private egress cm = CfFlatFileConnectionManager cm
    /// Transform a file connection manager definition
    let private mutate fn = ingress >> fn >> egress

    /// Encode a single character for use in flat file connection manager configuration
    let encodeCharacter (c:char) = 
      match c with
      | ' ' -> " "
      | '_' -> c |> int |> sprintf "_x%04X_"
      | _ when System.Char.IsLetterOrDigit(c) -> c.ToString()
      | _ -> c |> int |> sprintf "_x%04X_"

    /// Encodes a string for use in flat file connection manager configuration    
    let encodeString (s:string) =
      s.ToCharArray()
      |> Array.fold ( fun (sb : System.Text.StringBuilder) c -> sb.Append(encodeCharacter c) ) (System.Text.StringBuilder())
      |> fun sb -> sb.ToString()

    /// Create a new instance of a file connection manager definition
    let create name fileUsageType rowFormat connectionString =
        {
            CfFlatFileConnectionManager.description = ""
            name = name
            expressions = []

            connectionString = connectionString
            delayValidation = false
            fileUsageType = fileUsageType

            format = rowFormat
            localeId = None
            unicode = false
            codePage = System.Text.Encoding.Default.WindowsCodePage |> Some
            headerRowsToSkip = 0
            headerRowDelimiter = encodeString "\r\n"
            columnNamesInFirstDataRow = true
            rowDelimiter = ""
            dataRowsToSkip = 0
            textQualifier = encodeString "<none>" // This really is what SSIS encodes by default!

            columns = []
        }
        |> egress

    /// Transform a file connection manager definition by adding a property expression
    let addExpression targetProperty expr = assertType >> ConnectionManager.addExpression targetProperty expr 

    /// Transform a file connection manager definition by adding a property expression for the connection string property
    let addConnectionStringExpression expr = addExpression "ConnectionString" expr
        
    /// Get the file usage type
    let getFileUsageType = ingress >> fun cm -> cm.fileUsageType
    /// Set the file usage type
    let setFileUsageType fileUsageType = mutate <| fun cm -> { cm with fileUsageType = fileUsageType }
    
    /// Get the delay validation flag
    let getDelayValidation = ingress >> fun cm -> cm.delayValidation
    /// Set the delay validation flag
    let setDelayValidation delayValidation = mutate <| fun cm -> { cm with delayValidation = delayValidation }

    /// Get the connection string
    let getConnectionString = ingress >> fun cm -> cm.connectionString
    /// Set the connection string
    let setConnectionString connectionString = mutate <| fun cm -> { cm with connectionString = connectionString }

    /// Gets the flag that indicates that Unicode processing is applied
    let isUnicode = ingress >> fun cm -> cm.unicode
    /// Gets the code page, if any. Also used for UTF-7 and UTF-8
    let getCodePage = ingress >> fun cm -> cm.codePage

    /// Set the connection manager to use Unicode (UCS-2) encoding
    let setUnicode = mutate <| fun cm -> { cm with unicode = true ; codePage = None }
    /// Set the connection manager to use the specified code page.
    let setCodePage codePage = mutate <| fun cm -> { cm with unicode = false ; codePage = Some codePage }
    /// Set the connection manager to use the UTF-8 encoding
    let setUtf8 = setCodePage 65001

    /// Get the row format
    let getRowFormat = ingress >> fun cm -> cm.format
    /// Set the row format
    let setRowFormat rowFormat = mutate <| fun cm -> { cm with format = rowFormat }

    /// Get the locale id
    let getLocaleId = ingress >> fun cm -> cm.localeId
    /// Set the locale id
    let setLocaleId localeId = mutate <| fun cm -> { cm with localeId = localeId }

    /// Get the number of header rows to skip
    let getHeaderRowsToSkip = ingress >> fun cm -> cm.headerRowsToSkip
    /// Set the number of header rows to skip
    let setHeaderRowsToSkip rowsToSkip = mutate <| fun cm -> { cm with headerRowsToSkip = rowsToSkip }

    /// Get the number of data rows to skip
    let getDataRowsToSkip = ingress >> fun cm -> cm.dataRowsToSkip
    /// Set the number of data rows to skip
    let setDataRowsToSkip rowsToSkip = mutate <| fun cm -> { cm with dataRowsToSkip = rowsToSkip }

    /// Get the delimiter for header rows
    let getHeaderRowDelimiter = ingress >> fun cm -> cm.headerRowDelimiter
    /// Set the delimiter for header rows
    let setHeaderRowDelimiter applyEncoding delimiter = mutate <| fun cm -> { cm with headerRowDelimiter = applyEncoding |> (encodeString delimiter) @?@ delimiter }

    /// Get the column names in first data row flag
    let getColumnNamesInFirstDataRow = ingress >> fun cm -> cm.columnNamesInFirstDataRow
    /// Set the column names in first data row flag
    let setColumnNamesInFirstDataRow enabled = mutate <| fun cm -> { cm with columnNamesInFirstDataRow = enabled }

    /// Get the row delimiter.  For delimited files, this is encoded in the column delimiter
    let getRowDelimiter = ingress >> fun cm -> cm.rowDelimiter
    /// Set the row delimiter.  For delimited files, this is encoded in the column delimiter
    let setRowDelimiter applyEncoding delimiter = mutate <| fun cm -> { cm with rowDelimiter = applyEncoding |> (encodeString delimiter) @?@ delimiter }

    /// Get the text qualifier
    let getTextQualifier = ingress >> fun cm -> cm.textQualifier
    /// Set the text qualifier
    let setTextQualifier applyEncoding textQualifier = mutate <| fun cm -> { cm with textQualifier = applyEncoding |> (encodeString textQualifier) @?@ textQualifier }

    /// Get the columns
    let getColumns = ingress >> fun cm -> cm.columns
    /// Clear all columns
    let clearColumns = mutate <| fun cm -> { cm with columns = [] }
    /// Add a single column
    let addColumn column = mutate <| fun cm -> { cm with columns = cm.columns @ [column] } // preserve order
    /// Add multiple columns
    let addColumns columns = mutate <| fun cm -> { cm with columns = cm.columns @ columns } // preserve order

    /// Define a flat file column
    let defineColumn name columnFormat encodeDelimiter delimiter width maxWidth dataType precision scale isTextQualified =
      {
        name = name
        format = columnFormat
        delimiter = encodeDelimiter |> encodeString delimiter @?@ delimiter
        width = width
        maxWidth = maxWidth
        dataType = dataType
        precision = precision
        scale = scale
        textQualified = isTextQualified
      }

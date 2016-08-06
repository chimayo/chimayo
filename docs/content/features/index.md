Features
========

SQL Server support
------------------

Current SQL Server support is shown below:

<div class="panel panel-default">

<table class="table">
<tr><th>SQL Server version</th><th>Read</th><th>Write</th><th>Execute</th></tr>
<tr><th>2008 R2</th><td>Yes</td><td>No</td><td>No</td></tr>
<tr><th>2012</th><td>Yes</td><td>Yes</td><td>Yes</td></tr>
<tr><th>2014</th><td>No</td><td>No</td><td>Yes</td></tr>
<tr><th>2016</th><td>No</td><td>No</td><td>Yes</td></tr>
</trow>
</table>

</div>

The internal IDs used by different versions of SQL Server change and so it is not currently
possible to read or write packages produced by unsupported versions of SQL Server. Nevertheless,
the package formats are very similar and earlier package versions can be run by later versions of
SQL Server.

Full support for SQL Server 2014 and 2016 is on the roadmap. Delivery will depend very much on the
interest from the community to help deliver these features.

SSIS feature support
--------------------

Chimayo SSIS has been designed to support the entire feature set of SQL Server Integration Services,
but not all features are currently implemented.

### Unsupported

* Event handlers
* .NET scripting
* Thirdy-party components
* SQL Server Analysis Services

### General features

* Package configurations
* Connection managers
  * OleDB
  * ADO.NET
  * File
  * Flat file
* Logging
* Variables
* Expressions

### Control flow

* Precedence constraints
* Execute SQL task
* Execute package task
* Execute process task
* Expression task
* Sequence task
* ForLoop task
* ForEachLoop task
* Pipeline task (data flow)

### Data flow

* Sources
  * Flat file source
  * OleDB source
  * XML source
* Transforms
  * Aggregate
  * Conditional split
  * Derived column
  * Lookup
  * Multicast
  * Row count
  * Union all
* Destinations
  * OleDB destination
  * Recordset destination

Chimayo SSIS features
---------------------

* SQL Server 2008 R2 Integration Services package reader
* SQL Server 2012 Integration Services package reader
* SQL Server 2012 Integration Services package writer
* Package normalisation
* Package transforms
* F# code generation
* Idempotent round-tripping
* Package validation



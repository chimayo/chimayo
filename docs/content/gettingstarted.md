Getting started
===============

There are several ways to get started with Chimayo:

* Use the command line tool to examine or convert packages
* Create a new package from scratch using the Chimayo APIs
* Reverse engineer an existing package

The easiest way to get the Chimayo command line tool and binaries is via [NuGet](http://nuget.org/packages/Chimayo.Ssis).

Currently, Chimayo depends on .NET 4.6.

Using the command line tool
---------------------------

The command line tool is installed by NuGet into the `packages\Chimayo.Ssis.x.y.z\tools` folder as `Chimayo.Ssis.CommandLine.exe`.

If you run the tool without parameters, you get this help info:

```
Chimayo.Ssis.CommandLine.exe (x.y.z)

SQL Server Integration Services package transformations

  Commands

    /codegen [inputfile] [/o [outputfile]]   - Read an SSIS package and write an F# code file
    /roundtrip [inputfile] [/o [outputfile]] - Read an SSIS package and write another SSIS package
    /roundtripall [path]                     - Read an SSIS package and write another SSIS package for all files in folder/subtree
    /roundtripvalidate [inputfile]           - Validate roundtrip
    /validate [inputfile]                    - Validate package design
    /compare                                 - Compare two packages
        [/diff N]                              produces a diff style comparison with N context lines
        [/iv {2008|2012|2016}] inputfile       version and first input file
        [/iv {2008|2012|2016}] inputfile       version and second input file
    /version                                 - Show version information
    /? | /help | /h                          - Show this help

  Options

    inputfile                                - Source filename (default: stdin)
    /o outputfile                            - Destination filename (default: stdout)
    /iv {2008|2012|2016}                     - SSIS version for input file
    /ov {2008|2012|2016}                     - SSIS version for output file and for roundtrip test
    /ns namespace                            - Specify target namespace for generated code (default: GeneratedPackages)
    /r                                       - Enable recursion in /roundtripall
    /keep                                    - Keeps original files in /roundtripall

```

Using the `/codegen` option will let you see the content of a package as F# code. This may help to see how Chimayo is used as well as
showing what a package does.

Using the `/compare` option will allow you to compare two packages, even between SSIS versions. These are not
XML comparisons, they ignore lots of stuff you don't care about, like the order of information in the package, the
internal GUIDs and diagram data (and lots more) and they hopefully make things much clearer. Chimayo is designed
to have a single internal AST representation for any precise design and to have deterministic code generation.

Create a new package from scratch using the Chimayo API
-------------------------------------------------------

There are three steps to this:

1. Create a new .NET 4.6 F# console application project
2. Add the Chimayo.SSIS NuGet package
3. Write the package code

An example package is shown below:

```fsharp
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi
open Chimayo.Ssis.Writer2012

let pkg = 
    Package.create @"My package"
    |> Package.addVariables
        [
            Variables.createSimple @"User::myVar" 10
            Variables.createSimple @"User::myVar2" "String variable"
            Variables.create "CustomNamespace" "exprVar" false false "" (Some """ "a" + "b" """)
        ]
    |> Package.addConnectionManagers
        [
            OleDbConnectionManager.create "SQL"
                <| SqlConnectionStringHelper.createInlineOleDbConnectionString "." "tempdb" None "Chimayo SSIS Example"
        ]
    |> Package.addExecutables
        [
            let exes =
                [
                    ExecuteSql.createSimple "SQL1" "SQL" "PRINT 'HELLO'"
                    ExecuteSql.createSimple "SQL2" "SQL" "PRINT 'WORLD'"
                    ExecuteSql.createSimple "SQL3" "SQL" "PRINT 'FROM'"
                    ExecuteSql.createSimple "SQL4" "SQL" "PRINT 'CHIMAYO'"
                ]
            let final, priors = exes |> PrecedenceConstraints.chain CfExecutableResult.Success
            yield! priors
            yield final
        ]

[<EntryPoint>]
let main argv = 
    pkg
    |> Chimayo.Ssis.Writer2016.PackageBuilder.toString
    |> printf "%s\n" 
    System.Console.ReadLine() |>ignore
    0
```

As with most F# programs, it is easiest to read this bottom-up.

### main

```fsharp
[<EntryPoint>]
let main argv = 
    pkg
    |> Chimayo.Ssis.Writer2016.PackageBuilder.toString
    |> printf "%s\n" 
    System.Console.ReadLine() |>ignore
    0
```

This takes an existing value, `pkg`, and passes it to one of the SSIS 2012 Writer functions. There
are several functions available and this one directly dumps the SSIS XML to the console. Because
the .NET String type has utf-16 type internally, you can see the XML encoding is also utf-16. In
general, packages are directly written to disk as utf-8.

It then waits for input before exiting, because otherwise the window will close when run from Visual Studio!

### let pkg

```fsharp
let pkg = 
    Package.create @"My package"
    |> Package.addVariables ...
    |> Package.addConnectionManagers ...
    |> Package.addExecutables ...
```

Chimayo is designed for a normal functional programming experience. `Package.create` creates
a new instance of the `CftPackage` type that represents a single complete SSIS package. This
is a record type that has a number of properties and deeply nested collections to represent
the hierarchy of control flow tasks and any data flows (also called pipelines) that are defined.

Each of the functions here adds detail to one or more fields in the type. Each of these
fields are collections and are generally added to using lists, although in many cases
you can add one at a time using other functions in the module.

This code also demonstrates the general pattern in Chimayo, data types are prefixed
with one of `Cf`, `Cft` or `Df` for Control Flow, Control Flow Task or Data Flow
respectively. In SSIS terminology, a package is a kind of SSIS task, so that's why
it also has a `Cft` prefix. APIs do not have the prefix, but are otherwise similarly
named.

APIs are in the `Ast.ControlFlowApi` and `Ast.DataFlowApi` namespaces, and data
types are in the `Ast.ControlFlow` and `Ast.DataFlow` namespaces.

* See [CftPackage](reference/chimayo-ssis-ast-controlflow-cftpackage.html)
* See [Package](reference/chimayo-ssis-ast-controlflowapi-package.html)

### Variables

```fsharp
            Variables.createSimple @"User::myVar" 10
            Variables.createSimple @"User::myVar2" "String variable"
            Variables.create "CustomNamespace" "exprVar" false false "" (Some """ "a" + "b" """)
```

Variables, and other types, are typically created using helper methods in similarly
named modules in the ...Api namespaces, although you can create them directly using
the record type definitions if you prefer. The APIs are more resilient to change
in the data type design, but the data types are designed to be open and correlate to
the SSIS internal specifications for each type of object, to the extent that support
is implemented.

These three statements each create a single SSIS variable at the top level in the package.
Variables can also be added at task level, exactly the same as if you were creating
the package in the graphical designer.

The variables are:

* An integer variable in the namespace _User_ and named _myVar_ with the value 10
* A string variable in the namespace _User_ and named _myVar2_ with the value "String variable"
* A string variable in the namespace _CustomNamespace_ and named _exprVar_ which has the expresssion `"a" + "b"`
  and the initial value "" (which is immediately overwritten by the expression). The other two options
  are the defaults for _read only_ and _raise change events_

### Connection Managers

```fsharp
            OleDbConnectionManager.create "SQL"
                <| SqlConnectionStringHelper.createInlineOleDbConnectionString "." "tempdb" None "Chimayo SSIS Example"
```

This code adds an OleDB connection manager named _SQL_ to the package. Connection strings
can be manually defined, but in this case it is constructed via a helper function.

Currently, the code has support for creating SQL Server 2012 Native Client connections. You will need
to override the provider name if you want to use a different native client version.

This particular connection manager:

* Is named "SQL"
* Connects to the local server, "."
* Connects to the "tempdb" database
* Uses integrated security (the `None` option, can also be `Some` username and password tuple)

### Tasks

```fsharp
            let exes =
                [
                    ExecuteSql.createSimple "SQL1" "SQL" "PRINT 'HELLO'"
                    ExecuteSql.createSimple "SQL2" "SQL" "PRINT 'WORLD'"
                    ExecuteSql.createSimple "SQL3" "SQL" "PRINT 'FROM'"
                    ExecuteSql.createSimple "SQL4" "SQL" "PRINT 'CHIMAYO'"
                ]
            let final, priors = exes |> PrecedenceConstraints.chain CfExecutableResult.Success
            yield! priors
            yield final
```

Tasks, or executables, are control flow elements. This code works as follows:

* Lines 1 to 7 create four new ExecuteSQL tasks in a value `exes`
* Line 8 creates an ordered sequence of precedence constraints between the four
  tasks. It associates the updated tasks with two values, the first three tasks
  as `priors` and the final one as `final`. The difference here is because
  you typically want to attach further precedence constraints to the final
  task in the chain
* Lines 9 and 10 expose the four adapted tasks to be added to the package

There are many different types of executable but they can be added in the same way.
Similarly, precedence constraints can be added in a few ways and there is full support
for the variety of precedence constraints supported by SSIS.

Reveres engineer an existing package
------------------------------------

So long as you have an existing SSIS 2008 R2, SSIS 2012, or SSIS 2016 package that 
conforms with the feature set supported by Chimayo, you can reverse engineer it 
into F# code, and then you can also inspect the CftPackage instance that's created.

Just use the command line tool as explained above, for example:

```
Chimayo.Ssis.CommandLine.exe /codegen /iv 2008 MySsis2008Package.dtsx /o MySsis2008Package.fs
Chimayo.Ssis.CommandLine.exe /codegen /iv 2012 MySsis2012Package.dtsx /o MySsis2012Package.fs
Chimayo.Ssis.CommandLine.exe /codegen /iv 2016 MySsis2016Package.dtsx /o MySsis2016Package.fs
```

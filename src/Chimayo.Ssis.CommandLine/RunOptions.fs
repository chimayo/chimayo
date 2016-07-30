namespace Chimayo.Ssis.CommandLine

type Version =
    | Ssis2008
    | Ssis2012

[<RequireQualifiedAccessAttribute>]
type RunOptions =
    | Help
    | Version
    | CodeGen of (Version * string option) * string * string option // input version and filename, namespace, output filename -> can use stdin/stdout if not provided
    | RoundTrip of (Version * string option) * (Version * string option) // input version and filename, output version and filename -> can use stdin/stdout if not provided
    | RoundTripPath of Version * string option * bool * bool * Version // input version, path, recursion flag, keep originals flag and output version
    | ValidateRoundtrip of Version * Version * string option // input version, rountrip version, input filename-> can use stdin if not provided
    | ValidatePackage of Version * string option // input version and filename -> can use stdin if not provided
    | ComparePackage of (Version * string) * (Version * string) * (int option) // two packages and versions, and a diff context lines if diff mode is enabled, cannot use stdin
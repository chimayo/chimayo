namespace Chimayo.Ssis.Common

/// Useful top level functions
[<AutoOpen>]
module TopLevelFunctions =

    /// Reverse first two arguments to a curried function
    let swap f x y = f y x

    /// Curry a function taking a paired argument
    let curry f = fun a b -> f (a,b)

    /// Uncurry a function taking two consecutive curried arguments
    let uncurry f = fun (a,b) -> f a b

    /// System.String.Equals(InvariantCulture)
    let stringCompareInvariant (a:System.String) b = a.Equals(b, System.StringComparison.InvariantCulture)

    /// System.String.Equals(InvariantCultureIgnoreCase)
    let stringCompareInvariantIgnoreCase (a:System.String) b = a.Equals(b, System.StringComparison.InvariantCultureIgnoreCase)

    /// if 'test' then 'trueBranch' else 'falseBranch'
    let cond trueBranch falseBranch test = if test then trueBranch else falseBranch

    /// if 'test' then failwith 'exceptionMessage'
    let throwIfTrue test exceptionMessage = if test then failwith exceptionMessage else ()

    /// List.fold with several arguments reordered to support chaining
    let swapAndFoldList argReversedFolder items state =
        items |> List.fold (swap argReversedFolder) state

    /// Takes an option and returns a transformed value or a default value
    let optionOrDefaultMap mapper def o = o |> Option.fold (fun _ v -> mapper v) def

    /// Takes an option and returns its value or a default
    let optionOrDefault<'a> : ('a -> 'a option -> 'a) = optionOrDefaultMap id

    /// Takes an option and returns a list of zero or one elements with the element transformed
    let optionMapToList fn = Option.map fn >> Option.toList

    /// Converts an integer string to an enum member
    let inline stringInt32ToEnum (value:string) = value |> (int) |> enum

    /// Converts a string to an enum member
    let inline stringToEnum<'a> (value:string) : 'a = downcast System.Enum.Parse(typedefof<'a>, value)

    /// converts a value to an integer string
    let inline toInt32String value = value |> int |> string

    /// delays execution of a function, swallowing any input
    let inline defer x = fun _ -> x

    /// constructs a list of a single element
    let makeList element = [element]

    /// reverses syntax for function invocation
    let inline reverseInvoke arg fn = fn arg

    /// Appends a sequence to a list one element at a time (watch out for ordering implications)
    let inline appendSeqToList<'a> : list<'a> -> seq<'a> -> list<'a> = Seq.fold (List.Cons |> curry |> swap)

/// Useful operators
module CustomOperators =

    /// Allows constructs like 'test |> (doSomethingIfTrue @?@ doSomethingElseIfFalse)
    /// which is useful for supporting alternative behaviours in chained functions
    let inline (@?@) a b = fun test -> cond a b test

    /// Shorthand for conditional exception throwing: 'test @?! "exception message if true"'
    let inline (@?!) test exceptionMessage = throwIfTrue test exceptionMessage

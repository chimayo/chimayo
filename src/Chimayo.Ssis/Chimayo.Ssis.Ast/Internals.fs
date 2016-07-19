[<AutoOpen>]
module internal Chimayo.Ssis.Ast.Intrinsics

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

/// <summary>Adds an element to a internal keyed-F# list within an object, if a matching key is not found, based on a (T -> Key) mapping.</summary>
let tryAdd<'a,'key,'value> (collectionSelector:'a->'value list) keySelector (adder:'value->'a->'a) (comparer:'key->'key->bool) value targetObject =

        let coll = targetObject |> collectionSelector
        let key = value |> keySelector
        let conflict = coll |> List.map keySelector |> List.tryFind (comparer key)
        match conflict with
        | None -> targetObject |> adder value
        | _ -> sprintf "Failed to add object with key %A to collection due to name conflict" key |> failwith

/// Performs a selective mapping of items within a list if they match some provided criteria
let inline listMapIf fn test = List.map (fun x -> test x |> (fn x) @?@ x)

/// Perform List.find but nest KeyNotFound exceptions in an outer one with a more descriptive label, generated only when thrown
let listFindL fn labelFn xs = 
    try 
        List.find fn xs 
    with | :? System.Collections.Generic.KeyNotFoundException as knfe ->
        raise <| System.Collections.Generic.KeyNotFoundException(labelFn (), knfe)
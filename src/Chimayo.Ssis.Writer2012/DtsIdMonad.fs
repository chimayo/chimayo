module Chimayo.Ssis.Writer2012.DtsIdMonad

open Chimayo.Ssis.Common

module Internals =

    type DtsIdMonadStateType = int (* counter *) * Microsoft.FSharp.Collections.Map<string, System.Guid> * Set<string>


    type DtsIdMonad<'a> =
        DtsIdMonad of (DtsIdMonadStateType -> (DtsIdMonadStateType*'a))
        with member x.Execute() =
                        match x with 
                        | DtsIdMonad f -> f (1 , Map.empty, Set.empty) |> snd

    let plumbDtsId refId (c,map,requests) =
        let requests' = Set.union requests (refId |> Set.singleton)
        if map |> Map.containsKey refId then
            (c,map,requests'), (map |> Map.find refId)
        else
            let newGuid = System.Guid.NewGuid()
            let newMap = map |> Map.add refId newGuid
            (c,newMap,requests'), newGuid

    let setState x = DtsIdMonad (fun s -> x, ())
    let getState = DtsIdMonad (fun s -> s, s)

    let getDtsId key = DtsIdMonad ( fun s -> s |> plumbDtsId key )

    /// assignDtsId does not mark registrations as used; this means that they aren't reported as registrations
    /// unless they are subsequently accessed
    let assignDtsId key value = DtsIdMonad ( fun (c,map,requests) -> (c , map |> Map.add key value, requests) , () )

    let tryGetDtsId key = DtsIdMonad ( fun (c,m,requests) -> (c,m,requests) , m |> Map.tryFind key )

    let getUniqueCounterValue = DtsIdMonad (fun (c,m,requests) -> (c+1,m,requests) , c)

    let bind f (DtsIdMonad sf) =
        DtsIdMonad (fun s -> let (s', a) = sf s
                             let (DtsIdMonad f') = f a
                             f' s')


    let _return v = DtsIdMonad (fun s -> s,v)

    let map f (DtsIdMonad sf) =
        DtsIdMonad (fun s -> let (s', a) = sf s
                             let b = f a
                             s' , b)

    let combine expr1 expr2 =
        expr1 |> bind (fun () -> expr2)


    type DtsIdMonadBuilder() =
        member x.Return(v) = _return v
        member x.Bind(DtsIdMonad sf,f) = bind f (DtsIdMonad sf)
        member x.Run(DtsIdMonad sf) = DtsIdMonad (fun s -> sf s)
        member x.Zero() = DtsIdMonad (fun s -> s, ())
        member x.Combine(expr1,expr2) = combine expr1 expr2
        member x.ReturnFrom(v) = v


let dtsIdState = new Internals.DtsIdMonadBuilder()

module DtsIdState =
    let rec listmap f col =
            match col with
            | [] -> dtsIdState { return [] }
            | x::xs ->
                dtsIdState {
                    let! x' = f x
                    let! remainder = listmap f xs
                    return x' :: remainder
                }

    let rec listcollect f col =
        listmap f col |> Internals.map List.concat

    let rec listmapi f col =
        let rec mapi f col i =
            match col with
            | [] -> dtsIdState { return [] }
            | x::xs ->
                dtsIdState {
                    let! x' = f i x
                    let! remainder = mapi f xs (i+1)
                    return x' :: remainder
                }
        mapi f col 0

    let rec listchoose f col =
            match col with
            | [] -> dtsIdState { return [] }
            | x::xs ->
                dtsIdState {
                    let! x' = f x
                    let! remainder = listchoose f xs
                    match x' with
                    | Some v -> return v :: remainder
                    | _ -> return remainder
                }

    let rec listiter f col =
            match col with
            | [] -> dtsIdState { return () }
            | x::xs ->
                dtsIdState {
                      do! f x
                      do! listiter f xs
                }

    let rec listbind col =
        match col with
        | [] -> dtsIdState { return [] }
        | x::xs -> 
                dtsIdState {
                    let! x' = x
                    let! remainder = listbind xs
                    return x' :: remainder
                }
    let inline bindOption x = match x with | Some f -> f | None -> id

    let inline value x = dtsIdState { return x }
    let inline Some x = value (Option.Some x)
    let None<'T> : Internals.DtsIdMonad<'T option> = value (Option.None)

    let inline makeSome state = Internals.bind (Some) state

    let map = Internals.map

    let getDtsId = Internals.getDtsId
    let getUniqueCounterValue = Internals.getUniqueCounterValue
    let tryGetDtsId = Internals.tryGetDtsId

    let register registrations = 
        listiter (uncurry Internals.assignDtsId) registrations

    let getRegistrations =
      Internals.DtsIdMonad 
        (fun (c,m,r) -> 
          (c,m,r)
          ,
          m
          |> Map.toList
          |> List.filter (fun (n,_) -> r |> Set.contains n))


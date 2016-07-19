module Chimayo.Ssis.CodeGen.QuotationHelper

open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns


let getFullyQualifiedFunctionNameFromLambda : Expr -> string =
    let rec getMember =
        function
        | Lambda (_,x) -> getMember x
        | Let (_,_,x) -> getMember x
        | Call (_,x,_) -> Some (x:>System.Reflection.MemberInfo)
        | PropertyGet (_,x,_) -> Some (x:>System.Reflection.MemberInfo)
        | _ -> None
    getMember >> Option.map (fun mi -> sprintf "%s.%s" mi.DeclaringType.FullName mi.Name) >> Option.get

let getRecordMemberName : Expr -> string =
    function
    | Patterns.PropertyGet (_,y,_) -> y.Name
    | _ -> failwith "invalid"

module Chimayo.Ssis.CodeGen.TypeNameRegistry

let typeRegistry =
    [
        typedefof<Chimayo.Ssis.Ast.ControlFlow.CfIndirectSource>
        typedefof<Chimayo.Ssis.Ast.ControlFlow.CfForEachLoopFileNameRetrievalFormat>
        typedefof<Chimayo.Ssis.Ast.ControlFlow.CfForEachLoopNodeListMode>
        typedefof<Chimayo.Ssis.Ast.ControlFlow.CfForEachLoopNodeListInnerMode>
        typedefof<Chimayo.Ssis.Ast.ControlFlow.CfForEachLoopDataSetMode>

        typedefof<Chimayo.Ssis.Ast.ControlFlow.CfDataType>
        typedefof<Chimayo.Ssis.Ast.ControlFlow.CfExecuteSqlResult>
        typedefof<Chimayo.Ssis.Ast.ControlFlow.CfParameterDirection>
        typedefof<Chimayo.Ssis.Ast.ControlFlow.CfExpression>
        typedefof<Chimayo.Ssis.Ast.ControlFlow.CfExecutableResult>
        typedefof<Chimayo.Ssis.Ast.ControlFlow.CfTransactionOption>
        typedefof<Chimayo.Ssis.Ast.ControlFlow.CfIsolationLevel>
        typedefof<Chimayo.Ssis.Ast.ControlFlow.CfFileUsage>
        typedefof<Chimayo.Ssis.Ast.ControlFlow.CfFlatFileRowFormat>
        typedefof<Chimayo.Ssis.Ast.ControlFlow.CfFlatFileColumnFormat>
        typedefof<Chimayo.Ssis.Ast.ControlFlow.CfLogFilterKind>
        typedefof<Chimayo.Ssis.Ast.ControlFlow.CfLogMode>
        typedefof<Chimayo.Ssis.Ast.ControlFlow.CfWindowStyle>

        typedefof<Chimayo.Ssis.Ast.DataFlow.DfDataType>
        typedefof<Chimayo.Ssis.Ast.DataFlow.DfOutputColumnSpecialFlags>
        typedefof<Chimayo.Ssis.Ast.DataFlow.DfOutputColumnRowDisposition>
        typedefof<Chimayo.Ssis.Ast.DataFlow.DfComparisonFlags>
        typedefof<Chimayo.Ssis.Ast.DataFlow.DfExpression>
        typedefof<Chimayo.Ssis.Ast.DataFlow.DfSubExpression>
        typedefof<Chimayo.Ssis.Ast.DataFlow.DfDerivedColumnColumnBehaviour>
        typedefof<Chimayo.Ssis.Ast.DataFlow.DfOleDbDestinationFastLoadOption>
        typedefof<Chimayo.Ssis.Ast.DataFlow.DfOleDbDestinationTarget>
        typedefof<Chimayo.Ssis.Ast.DataFlow.DfOleDbSourceInput>
        typedefof<Chimayo.Ssis.Ast.DataFlow.DfParameterDirection>
        typedefof<Chimayo.Ssis.Ast.DataFlow.DfLookupCacheMode>
        typedefof<Chimayo.Ssis.Ast.DataFlow.DfAggregateScaling>
        typedefof<Chimayo.Ssis.Ast.DataFlow.DfAggregateOperation>
        typedefof<Chimayo.Ssis.Ast.DataFlow.DfFlatFileSourceCodePage>
        typedefof<Chimayo.Ssis.Ast.DataFlow.DfXmlSourceIntegerMode>
        typedefof<Chimayo.Ssis.Ast.DataFlow.DfXmlSourceMapping>
        typedefof<Chimayo.Ssis.Ast.DataFlow.DfXmlSourceSchemaSource>
    ]
    |> List.map (fun t -> t.FullName |> fun s -> t, s.Replace("+", "."))
    |> List.fold (fun m (k,v) -> m |> Map.add k.FullName v) Map.empty

let nameRegistry =
    [ 
        "Chimayo.Ssis.Ast.DataFlow.DfDataType", "DfDataType"
        "Chimayo.Ssis.Ast.DataFlow.DfInputConnection.DfInputConnection", "DfInputConnection"
        "Chimayo.Ssis.Ast.DataFlow.DfOutputReference.DfOutputReference", "DfOutputReference"
        "Chimayo.Ssis.Ast.DataFlow.DfInputColumnReference.DfInputColumnReference", "DfInputColumnReference"
        "Chimayo.Ssis.Ast.DataFlow.DfNamedEntity.DfName", "DfName"
    ]
    |> List.fold (fun m (k,v) -> m |> Map.add k v) Map.empty

let inline typeToName value = 
    typeRegistry 
    |> Map.tryFind (value.GetType().FullName)
    |> function | Some x -> x | _ -> failwith (sprintf "Unable to locate '%s' in typeRegistry" (value.GetType().FullName))
let inline innerTypeToName value = 
    let t = value.GetType()
    let dt = t.DeclaringType
    match dt with
    | null -> invalidOp (sprintf "Attempt to get a declaring type when none exists for '%s'" t.FullName)
    | _ -> 
        typeRegistry 
        |> Map.tryFind (dt.FullName)
        |> function | Some x -> x | _ -> failwith (sprintf "Unable to locate '%s' in typeRegistry" dt.FullName)
let inline nameLookup key = nameRegistry |> Map.find key

let imports =
    [
        "Chimayo.Ssis.Ast.ControlFlow"
        "Chimayo.Ssis.Ast.DataFlow"
        "Chimayo.Ssis.Ast.ControlFlowApi"
        "Chimayo.Ssis.Ast.DataFlowApi"
    ]

let aliases = // source * asImportedModule * replacement
    [
        "Chimayo.Ssis.Ast.ControlFlowApi", false, ""
        "Chimayo.Ssis.Ast.DataFlowApi", false, ""

        "Chimayo.Ssis.Ast.ControlFlow", false, ""
        "Chimayo.Ssis.Ast.ControlFlow._CfData.T", false, "CfData"
        "Chimayo.Ssis.Ast.ControlFlow._CfParameterDirection.T", false, "CfParameterDirection"
        "Chimayo.Ssis.Ast.ControlFlow._CfExecuteSqlResult.T", false, "CfExecuteSqlResult"
        "Chimayo.Ssis.Ast.ControlFlow._CfPrecedenceConstraintLogic.T", false, "CfPrecedenceConstraintLogic"
        "Chimayo.Ssis.Ast.ControlFlow._CfPrecedenceConstraints.T", false, "CfPrecedenceConstraints"
        "Chimayo.Ssis.Ast.ControlFlow._CfWindowStyle.T", false, "CfWindowStyle"
        
        "Chimayo.Ssis.Ast.DataFlow", false, ""
        "Chimayo.Ssis.Ast.DataFlow._DfDataType.T", false, "DfDataType"
        "Chimayo.Ssis.Ast.DataFlow._DfOutputColumnRowDisposition.T", false, "DfOutputColumnRowDisposition"
        "Chimayo.Ssis.Ast.DataFlow._DfDerivedColumnColumnBehaviour.T", false, "DfDerivedColumnColumnBehaviour"
        "Chimayo.Ssis.Ast.DataFlow._DfOleDbDestinationFastLoadOption.T", false, "DfOleDbDestinationFastLoadOption"
        "Chimayo.Ssis.Ast.DataFlow._DfOleDbDestinationTarget.T", false, "DfOleDbDestinationTarget"
        "Chimayo.Ssis.Ast.DataFlow._DfOleDbSourceInput.T", false, "DfOleDbSourceInput"
        "Chimayo.Ssis.Ast.DataFlow._DfLookupCacheMode.T", false, "DfLookupCacheMode"
        "Chimayo.Ssis.Ast.DataFlow._DfAggregateScaling.T", false, "DfAggregateScaling"
        "Chimayo.Ssis.Ast.DataFlow._DfAggregateOperation.T", false, "DfAggregateOperation"
        "Chimayo.Ssis.Ast.DataFlow._DfFlatFileSourceCodePage.T", false, "DfFlatFileSourceCodePage"
        "Chimayo.Ssis.Ast.DataFlow._DfFlatFileSourceCodePage.T._Unicode", false, "DfFlatFileSourceCodePage.Unicode" // Unclear why this case is treated differently
        "Chimayo.Ssis.Ast.DataFlow._DfXmlSourceIntegerMode.T", false, "DfXmlSourceIntegerMode"
        "Chimayo.Ssis.Ast.DataFlow._DfXmlSourceMapping.T", false, "DfXmlSourceMapping"
        "Chimayo.Ssis.Ast.DataFlow._DfXmlSourceSchemaSource.T", false, "DfXmlSourceSchemaSource"
        "Chimayo.Ssis.Ast.DataFlow._DfXmlSourceSchemaSource.T._InlineSchema", false, "DfXmlSourceSchemaSource.InlineSchema" // Unclear why this case is treated differently

        "Chimayo.Ssis.Ast.DataFlow.DfDataType", false, "DfDataType"
        "Chimayo.Ssis.Ast.DataFlow.DfInputConnection.DfInputConnection", false, "DfInputConnection"
        "Chimayo.Ssis.Ast.DataFlow.DfOutputReference.DfOutputReference", false, "DfOutputReference"
        "Chimayo.Ssis.Ast.DataFlow.DfInputColumnReference.DfInputColumnReference", false, "DfInputColumnReference"
        "Chimayo.Ssis.Ast.DataFlow.DfNamedEntity.DfName", false, "DfName"
        "Chimayo.Ssis.Ast.DataFlow.DfComponentReference.DfComponentReference", false, "DfComponentReference"
        "Chimayo.Ssis.Ast.DataFlow.DfExpression.DfExpression", false, "DfExpression"
        "Chimayo.Ssis.Ast.DataFlow.DfSubExpression.Dfe", false, "Dfe"
        "Chimayo.Ssis.Ast.DataFlow.DfSubExpression.DfeQuoted", false, "DfeQuoted"
        "Chimayo.Ssis.Ast.DataFlow.DfSubExpression.DfeColumnRef", false, "DfeColumnRef"
    ]

let toAlias =
    let map = aliases |> List.fold (fun m (k,_,v) -> m |> Map.add k v) Map.empty
    fun name -> map |> Map.tryFind name |> function | Some x -> x | None -> name

let maybeToAlias =
    let map = aliases |> List.fold (fun m (k,_,v) -> m |> Map.add k v) Map.empty
    fun name -> map |> Map.tryFind name


let aliasName (name:string) =
    let join (ss: string[]) : string = System.String.Join(".", ss)
    let parts : string[] = name.Split('.')
    let rec evaluate partCount =
        if partCount = 0 
        then name
        else
            let prefix, suffix = join parts.[0..partCount - 1], join parts.[partCount..parts.Length - 1]
            match maybeToAlias prefix, suffix with
            | Some "", _ -> suffix
            | Some alias, "" -> alias
            | Some alias, _ -> join [| alias ; suffix |]
            | None, _ -> evaluate (partCount - 1)
    evaluate parts.Length
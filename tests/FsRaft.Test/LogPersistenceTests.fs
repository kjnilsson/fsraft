module LogPersistanceTests

#nowarn "25"

open System
open System.IO
open NUnit.Framework
open FsRaft
open FsRaft.LogPersistence

type TestType =
    { Maybe : int option
      Message : string }
    static member create x m =
        { Maybe = Some x
          Message = m }

let assertEqual x y =
    if x <> y then
        failwith (sprintf "expected %A got %A" x y)

let addRecord context n m =
    writeRecord context (n, 1L, TestType.create n m :> obj)

let addRecords context n =
    [context.NextIndex .. n]
    |> List.fold (fun s x ->
        addRecord s x (string n)) context

[<Test>]
let ``writeRecord should return an updated index when writing first record`` () =
    use stream = new MemoryStream () 
    let context = makeContext stream
    let thing : Record =  (1,1L,TestType.create 1 "hello" :> obj)
    let result = writeRecord context thing 
    Assert.AreEqual(1, result.Index.Count )
    Assert.AreEqual(2, result.NextIndex)
    match tryGet result 1 with
    | Some x when x = thing -> ()

[<Test>]
let ``writeRecord should return an updated index when writing another record`` () =
    use stream = new MemoryStream () 
    let context = makeContext stream
    let context = addRecord context 1 "test" 
    let expectedPosition = stream.Position
    stream.Position <- 0L //manually move position as this could happen
    let result = writeRecord context (context.NextIndex,1L,TestType.create 1 "hello" :> obj)
    
    assertEqual 3 result.NextIndex 
    assertEqual 2 result.Index.Count
    assertEqual expectedPosition (snd <| result.Index.Item 2)

[<Test>]
let ``writeRecord should overwrite index if it exists and remove all higher indices`` () =
    use stream = new MemoryStream () 
    let context = makeContext stream
    let context = addRecords context 10

    let record : Record = (5, 1L, TestType.create 5 "overwrite" :> obj)
    let result = writeRecord context record 
    
    assertEqual 6 result.NextIndex 
    assertEqual 5 result.Index.Count 
    match tryGet result 5 with
    | Some r -> assertEqual record r

[<Test>]
let ``tryGet should return none if index not present`` () =
    use stream = new MemoryStream () 
    let context = makeContext stream
    let context = addRecords context 10
    let result = tryGet context 11
    match result with
    | None -> () 

[<Test>]
let ``tryGet should resurn Some item if index present`` () =
    use stream = new MemoryStream () 
    let context = makeContext stream
    let context = addRecords context 10
    let result = tryGet context 5
    match result with
    | Some (index, _,_) -> assertEqual 5 index

[<Test>]
let ``query From should be inclusive of start index`` () =
    use stream = new MemoryStream () 
    let context = 
        makeContext stream
        |> fun x -> addRecords x 10
    let result = query context (From 5) |> Seq.toList

    match result with
    | [5,_,_;6,_,_;7,_,_;8,_,_;9,_,_;10,_,_] -> ()

[<Test>]
let ``query Range should be non inclusive of start index and inclusive of end index`` () =
    use stream = new MemoryStream () 
    let context = 
        makeContext stream
        |> fun x -> addRecords x 10
    let result = query context (Range(5, 8)) |> Seq.toList

    match result with
    | [6,_,_;7,_,_;8,_,_] -> ()
    
[<Test>]
let ``query All should return all available records`` () = 
    use stream = new MemoryStream () 
    let context = 
        makeContext stream
        |> fun x -> addRecords x 5
    let result = query context All |> Seq.toList

    match result with
    | [1,_,_;2,_,_;3,_,_;4,_,_;5,_,_] -> ()
    
[<Test>]
let ``makeContext should recreate the index`` () =
    use stream = new MemoryStream () 
    let context = 
        makeContext stream
        |> fun x -> addRecords x 5
    let result = makeContext stream
    assertEqual context.Index result.Index
    assertEqual context.NextIndex result.NextIndex

let protect f =
    try f() |> Some
    with
    | _ -> None

[<Test>]
let ``writeRecord should not allow gaps in the record history to be written`` () =
    use stream = new MemoryStream () 
    let context = makeContext stream
    let thing : Record =  (context.NextIndex + 1,1L,TestType.create 1 "hello" :> obj)
    match protect (fun () -> writeRecord context thing) with
    | None -> ()


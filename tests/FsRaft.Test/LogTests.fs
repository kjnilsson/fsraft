module LogTests
#nowarn "25"

open System
open System.IO
open NUnit.Framework
open FsRaft
open FsRaft.Persistence

type TestType =
    { Maybe : int option
      Message : string }
    static member create x m =
        { Maybe = Some x
          Message = m }

let assertEqual x y =
    if x <> y then
        failwith (sprintf "expected %A got %A" x y)

let addRecord context n t m =
    writeRecord context (n, t, TestType.create n m :> obj)

let addRecords n context =
    [context.NextIndex .. n]
    |> List.fold (fun s x ->
        addRecord s x (int64 x) (string n)) context

[<Test>]
let ``hasEntryFromTerm should return false for empty index`` () =
    use stream = new MemoryStream()
    let context = makeContext stream 
    let result = Log.hasEntryFromTerm context 5L
    assertEqual false result

[<TestCase(0, false)>]
[<TestCase(1, true)>]
[<TestCase(2, true)>]
[<TestCase(3, true)>]
[<TestCase(4, true)>]
[<TestCase(5, true)>]
[<TestCase(6, false)>]
let ``hasEntryFromTerm test case`` term expected =
    use stream = new MemoryStream()
    let context = makeContext stream |> addRecords 5
    let result = Log.hasEntryFromTerm context term
    assertEqual expected result


[<Test>]
let ``append should truncate older logs`` () =
    use stream = new MemoryStream()
    let context = makeContext stream |> addRecords 5

    let entries = [{ TermIndex = TermIndex.create 1L 1
                     Content = LogContent.Command ("data"B) }]

    let result = Log.append context entries

    assertEqual 2 result.NextIndex

[<Test>]
let ``truncate should remove all items higher than index passed in`` () =
    use stream = new MemoryStream()
    let context = makeContext stream |> addRecords 5

    let result = Log.truncate context 3

    assertEqual 4 result.NextIndex
    assertEqual 3 result.Index.Count
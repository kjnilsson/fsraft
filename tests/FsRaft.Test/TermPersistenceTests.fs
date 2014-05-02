module TermPersistenceTests


#nowarn "25"

open System
open System.IO
open NUnit.Framework
open FsRaft
open FsRaft.TermPersistence


let assertEqual x y =
    if x <> y then
        failwith (sprintf "expected %A got %A" x y)

[<Test>]
let ``TermContext should initialise to (0, None) when no persistent data available`` () =
    use stream = new MemoryStream()

    let context = TermContext stream
    assertEqual 0L context.Current
    assertEqual None context.VotedFor

[<Test>]
let ``TermContext should initialise previously written values`` () =
    use stream = new MemoryStream()
    let votedFor = Guid.NewGuid()
    let context = TermContext stream
    context.Current <- 1L
    context.VotedFor <- Some votedFor

    let result = TermContext stream

    assertEqual 1L result.Current
    assertEqual (Some votedFor) result.VotedFor

[<Test>]
let ``TermContext should reset votedFor when term is incremented`` () =
    use stream = new MemoryStream()
    let votedFor = Guid.NewGuid()
    let context = TermContext stream
    context.Current <- 1L
    context.VotedFor <- Some votedFor
    
    assertEqual 1L context.Current
    assertEqual (Some votedFor) context.VotedFor

    context.Current <- 2L

    assertEqual 2L context.Current
    assertEqual None context.VotedFor

[<Test>]
let ``TermContext should not reset voted when term is set to curren value`` () =
    use stream = new MemoryStream()
    let votedFor = Guid.NewGuid()
    let context = TermContext stream
    context.Current <- 1L
    context.VotedFor <- Some votedFor
    
    assertEqual 1L context.Current
    assertEqual (Some votedFor) context.VotedFor

    context.Current <- 1L

    assertEqual 1L context.Current
    assertEqual (Some votedFor) context.VotedFor

    
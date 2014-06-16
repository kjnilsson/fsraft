#r "bin/Debug/FSharpx.Core.dll"
#r "bin/Debug/FsPickler.dll"
#r "bin/Debug/Zorrillo.Shared.Logging.dll"
#r "../Zorrillo.Platform.Core/bin/Debug/Zorrillo.Platform.Common.dll"
#r "../Zorrillo.Platform.Core/bin/Debug/Zorrillo.Platform.Core.dll"

#load "Prelude.fs"
#load "Persistence.fs"
open Zorrillo.Platform.Raft
#load "Model.fs"
open Zorrillo.Platform.Raft
#load "Raft.fs"

open System
open System.IO
open FsPickler
open Zorrillo.Platform.Raft.LogPersistence


#time
open Zorrillo.Platform.Raft
open Zorrillo.Platform.Raft.LogPersistence

let file = new IO.FileStream(@"C:\dump\raftperf\169\e32b8a84-ebde-4803-a7d8-e4917517a28b\log.dat", IO.FileMode.OpenOrCreate,IO.FileAccess.ReadWrite)
let termFile = new IO.FileStream(@"C:\dump\raftperf\169\e32b8a84-ebde-4803-a7d8-e4917517a28b\term.dat", IO.FileMode.OpenOrCreate,IO.FileAccess.ReadWrite)
//let file = new IO.FileStream(@"C:\dump\raftperf\147\bf37efeb-17f7-4988-8ea2-6a90276c3167\log.dat", IO.FileMode.OpenOrCreate,IO.FileAccess.ReadWrite)
//let termFile = new IO.FileStream(@"C:\dump\raftperf\147\bf37efeb-17f7-4988-8ea2-6a90276c3167\term.dat", IO.FileMode.OpenOrCreate,IO.FileAccess.ReadWrite)
//let file = new IO.FileStream(@"C:\dump\raftperf\189\063d81f7-6f6b-4e79-8459-16e8f990e17b\log.dat", IO.FileMode.OpenOrCreate,IO.FileAccess.ReadWrite)
//let termFile = new IO.FileStream(@"C:\dump\raftperf\189\063d81f7-6f6b-4e79-8459-16e8f990e17b\term.dat", IO.FileMode.OpenOrCreate,IO.FileAccess.ReadWrite)
//let file = new IO.FileStream(@"C:\dump\raftperf\39\ae91189d-0fc8-4515-b303-db8dcbe3f61d\log.dat", IO.FileMode.OpenOrCreate,IO.FileAccess.ReadWrite)
//let termFile = new IO.FileStream(@"C:\dump\raftperf\39\ae91189d-0fc8-4515-b303-db8dcbe3f61d\term.dat", IO.FileMode.OpenOrCreate,IO.FileAccess.ReadWrite)
let context = makeContext file
let termContext = TermPersistence.TermContext (termFile)
context



let file_compacted = new IO.FileStream(@"C:\dump\raftperf\169\e32b8a84-ebde-4803-a7d8-e4917517a28b\log_compact2.dat", IO.FileMode.OpenOrCreate,IO.FileAccess.ReadWrite)

copyAndCompact file file_compacted

let xx = makeContext file_compacted

let cc = makeContext file_compacted

context.Index
|> Map.toSeq
|> Seq.fold (fun s (i, (t, _)) -> 
    let e = Log.tryGet context i
    writeRecord s (i, t, e.Value.Content)) cc

file_compacted.Dispose()




let rs = 
    RaftState.create (Guid "ae91189d0fc84515b303db8dcbe3f61d") context termContext 
//    |> applyLogsFollower (fun a b -> ()) (context.NextIndex - 1)

open Zorrillo.Platform.Raft.Model

Log.query rs.Log (From 10000) |> Seq.find (fun x -> true)

Log.tryGet rs.Log 10000

let (_,_,cmd) = query context (From (rs.Log.NextIndex - 1)) |> Seq.find (fun x -> true)
let lc = cmd :?> LogContent
evaluate (fun a b -> printfn ".") rs

cmd.GetType().Assembly
typeof<LogContent>.Assembly
context.Index.Item (context.NextIndex - 1)
//let c =
//    [0 .. 100000]
//    |> List.fold (fun (s : LogContext) x -> writeRecord s (s.NextIndex,int64 <| (float) x / 10000.0, x)) context
let read s : Record = 
    pickler.Deserialize(s)
file.Position <- 0L
read file;;
[0 .. 100] |> List.iter (fun x -> read file |> ignore)
[0 .. 10000] |> List.iter (fun x -> read file |> ignore)
[0 .. 10000] |> List.iter (fun x -> read file |> ignore)
[0 .. 10000] |> List.iter (fun x -> read file |> ignore)
[0 .. 10000] |> List.iter (fun x -> read file |> ignore)
[0 .. 10000] |> List.iter (fun x -> read file |> ignore)




let term = 7L
Map.tryFindKey (fun k (t,_) -> t = term) context.Index

query context (From (13113)) |> Seq.truncate 5 |> Seq.toList


    
//00:01:06.582, CPU: 00:01:07.486, GC gen0: 3844, gen1: 2383, gen2: 3
query c (From (100000)) |> Seq.truncate 5 |> Seq.toList
query c (From (100000)) |> Seq.toList
Map.tryFind 59999 c.Index
let context' = makeContext file

query context' (From (10000)) |> Seq.truncate 5 |> Seq.toList
Map.tryFind 59999 context'.Index
context' <> c

writeRecord c (c.NextIndex, 3L, c.NextIndex)

let c' =
    [c.NextIndex .. 100000]
    |> List.fold (fun (s : LogContext) x -> writeRecord s (s.NextIndex, 3L, x)) context
writeRecord c' (c'.NextIndex, 3L, c'.NextIndex)
file.Dispose()


//let log = @"C:\Zorrillo.Platform\Data\f08be72e-dacc-45ee-941a-cf03a737cb47\log.dat"
//let fs = new IO.FileStream(log, IO.FileMode.OpenOrCreate,IO.FileAccess.ReadWrite)
//
//let read s : Record = 
//    pickler.Deserialize(s)
//
//let readIndex (s : Stream) =
//    debug "Reading index..."
//    s.Position <- 0L
//    let rec inner index last =
//        let pos = s.Position
//        match protect (fun () -> read s) with
//        | Some (idx, term, _) -> 
//            let trimmed = 
//                Map.add idx (term, pos) index 
//                |> Map.filter (fun k _ -> idx >= k)
//            inner trimmed idx
//        | None -> index, last
//    inner Map.empty<int, int64 * int64> 0
//
//let context = makeContext fs
//
//let all = query context All |> Seq.toList
let e = Event<string>()
e.Publish.Add (fun _ -> System.Threading.Thread.Sleep 1000; printfn "yo")
e.Trigger ""
printfn "hellpw"

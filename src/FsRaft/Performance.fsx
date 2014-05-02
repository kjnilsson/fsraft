
#r "bin/Debug/FSharpx.Core.dll"
#r "bin/Debug/FsPickler.dll"
#r "bin/Debug/Zorrillo.Shared.Logging.dll"
#r "bin/Debug/Zorrillo.Platform.Raft.dll"
#r "../Zorrillo.Platform.Core/bin/Debug/Zorrillo.Platform.Common.dll"
#r "../Zorrillo.Platform.Core/bin/Debug/Zorrillo.Platform.Core.dll"


open Zorrillo.Platform.Raft
open System
open System.IO
open FsPickler
open Zorrillo.Platform.Raft.LogPersistence


open System
open System.IO
open FsPickler
open Zorrillo.Platform.Raft.LogPersistence
#time
open Zorrillo.Platform.Raft
open Zorrillo.Platform.Raft.LogPersistence

let file = new IO.FileStream(@"C:\dump\raftperf\169\e32b8a84-ebde-4803-a7d8-e4917517a28b\log.dat", IO.FileMode.OpenOrCreate,IO.FileAccess.ReadWrite)
let termFile = new IO.FileStream(@"C:\dump\raftperf\169\e32b8a84-ebde-4803-a7d8-e4917517a28b\term.dat", IO.FileMode.OpenOrCreate,IO.FileAccess.ReadWrite)
let file_compacted = new IO.FileStream(@"C:\dump\raftperf\169\e32b8a84-ebde-4803-a7d8-e4917517a28b\log_compact.dat", IO.FileMode.OpenOrCreate,IO.FileAccess.ReadWrite)
let cc = makeContext file_compacted

let context = makeContext file
context.Index
|> Map.toSeq
|> Seq.fold (fun s (i, (t, _)) -> 
    let e = Log.tryGet context i
    writeRecord s (i, t, e.Value.Content)) cc

file_compacted.Dispose()


let termContext = TermPersistence.TermContext (termFile)
let state = 
    RaftState.create (Guid "ae91189d0fc84515b303db8dcbe3f61d") context termContext 
    |> applyLogsFollower (fun a b -> ()) (context.NextIndex - 1)

Log.query state.Log (Range (5000, 5005)) |> Seq.toList |> ignore
query state.Log (From (5000)) |> Seq.truncate 5 |> Seq.toList |> ignore

Log.query state.Log (Range(5000, 5005)) |> Seq.toList |> List.length

Map.find 5000 state.Log.Index
evaluate (fun a b -> printfn ".") state |> ignore

let hb = new Heartbeat (Guid.NewGuid(), (fun a b -> printfn "."), state)
hb.State  state

let changed (s1 : RaftState) (s2 : RaftState) =
    s1.CommitIndex <> s2.CommitIndex
    || s1.Log.NextIndex <> s2.Log.NextIndex
    || s1.Term <> s2.Term
    || s1.Config <> s2.Config

state <> state
changed state state
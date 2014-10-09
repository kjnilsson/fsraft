namespace FsRaft.Test
#nowarn "25"

open System
open System.IO
open NUnit.Framework
open FsRaft
open FsRaft.Persistence


module RaftTests_castVote =

    open Messages

    let assertEqual x y =
        if x <> y then
            failwith (sprintf "expected %A got %A" x y)

    [<Test>]
    let ``castVote should not grant vote when candidates last log index is lower than peers last log index`` () =
        let their = { Term = 1L; Index = 10 }
        let our = { Term = 1L; Index = 11 }

        let result = Raft.castVote 1L our their

        match result with
        | None -> ()

    [<Test>]
    let ``castVote should grant vote if candidates last log term is greater than ours`` () =
        let their = { Term = 2L; Index = 11 }
        let our = { Term = 1L; Index = 10 }

        let expected = { Term = 2L; VoteGranted = true }
        let result = Raft.castVote 2L our their

        match result with
        | Some x when x = expected -> ()

    [<Test>]
    let ``castVote should grant vote if candidates last log term is the same as ours and their log index is at least as great as ours`` () =
        let their = { Term = 2L; Index = 10 }
        let our = { Term = 2L; Index = 10 }

        let expected = { Term = 2L; VoteGranted = true }
        let result = Raft.castVote 2L our their

        match result with
        | Some x when x = expected -> ()

    [<Test>]
    let ``castVote should not grant vote if requesters term is higher but index is lower`` () =
        let their = { Term = 2L; Index = 5 }
        let our = { Term = 2L; Index = 10 }

        let expected = { Term = 5L; VoteGranted = true }
        let result = Raft.castVote 5L our their

        assertEqual None result  


    let addEntry index term =
        { TermIndex = TermIndex.create term index
          Content = Command (string index |> Text.Encoding.UTF8.GetBytes) }

    let addEntries n (state : RaftState<_>) =
        [ state.Log.NextIndex .. state.Log.NextIndex + n - 1]
        |> List.fold (fun s x ->
            { s with Log = Log.add s.Log (addEntry x state.Term.Current) }) state

    [<Test>]
    let ``applyLogs should not apply a command entries twice when commit index is incremented`` () =
        let id = Guid.NewGuid(), "", 0
        use stream = new MemoryStream () 
        use termStream = new MemoryStream () 
        let context = makeContext stream
        let termContext = new TermContext (termStream)

        let appliedCount = ref 0
        let apply = 
            fun _ _ -> appliedCount := !appliedCount + 1; !appliedCount

        let raftState = RaftState<int>.create id 0 context termContext |> addEntries 5
         
        let result, _ = Raft.applyLogs ignore false (fun c s -> s) apply 2 raftState
        assertEqual 2 (!appliedCount)
        let result = Raft.applyLogs ignore false (fun c s -> s) apply 5 result
        assertEqual 5 (!appliedCount)

    [<Test>]
    let ``applyLogs follower should not advance commit index beyond the highest index of all received logs`` () =
        let id = Guid.NewGuid(), "", 0
        use stream = new MemoryStream () 
        use termStream = new MemoryStream () 
        let context = makeContext stream
        let termContext = new TermContext(termStream)
        let apply = fun _ _ -> 0

        let raftState = RaftState<_>.create id 0 context termContext |> addEntries 5

        let result, _ = Raft.applyLogs ignore false (fun _ s -> s) apply 7 raftState
        assertEqual 5 result.CommitIndex

    [<Test>]
    let ``applyConfigLeader should only move to Normal configuration if the state is in Joint mode and the log matches the configuration`` () =
        let id = Guid.NewGuid()
        use stream = new MemoryStream () 
        let context = makeContext stream
        use termStream = new MemoryStream () 
        let termContext = new TermContext (termStream)

        let peer1Id, peer1 = (Guid.NewGuid(), "", 0), Peer.create()
        let peer2Id, peer2 = (Guid.NewGuid(), "", 0), Peer.create()
        let c1 = { Peers = [peer1Id, peer1] |> Map.ofList }
        let c2 = { Peers = [peer1Id, peer1; peer2Id, peer2] |> Map.ofList } //next index change here is to simulate progress being made for replication of other logs
        let c2' = { c2 with Peers = c2.Peers |> Map.map (fun k v -> {v with NextIndex = 10 }) }
        let config = Joint(c1, c2)
        let state = RaftState<_>.create (Guid.NewGuid(), "", 0) 0 context termContext
        let stateConfig = Joint(c1, c2' )
        let state = { state with Config = stateConfig }
        
        let result = Raft.applyConfigLeader ignore config state
        match result.Config with
        | Normal n -> 
            assertEqual n c2'
            assertEqual 1 result.Log.Index.Count
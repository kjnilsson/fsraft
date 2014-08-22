namespace FsRaft

open System
open FSharpx
open FSharpx.State
open FSharpx.Lens.Operators

[<AutoOpen>]
module Model =

    type Endpoint = Guid * string * int

    type TermIndex =
        { Term : int64
          Index : int }

        static member create term index =
            { Term = term
              Index = index }

        static member Default = 
            TermIndex.create 0L 0

    and Entry = 
        { TermIndex : TermIndex
          Content : LogContent }

    and LogContent =
        | Command of byte[]
        | ConfigEntry of Configuration

    and Cluster =
        { Peers : Map<Endpoint, Peer> }

    and Peer =
        { NextIndex : int
          MatchIndex : int }
        static member create () =
            { NextIndex = 0
              MatchIndex = 0 }

    and Configuration =
        | Normal of Cluster
        | Joint of Cluster * Cluster // old * new

    open FsRaft.Persistence

    type RaftState = 
        { Id : Endpoint
          Term : TermContext
          Leader : Endpoint option
          Log : LogContext 
          CommitIndex : int
          Config : Configuration }
        static member create id logContext termContext =
          { Id = id
            Term = termContext
            Log = logContext
            Leader = None
            CommitIndex = 0
            Config = Normal { Peers = Map.empty } }
        interface IDisposable with
            member x.Dispose () =
                dispose x.Log
                dispose x.Term

module Log =
    open FsRaft.Persistence

    let add context (entry : Entry) =
        writeRecord context (entry.TermIndex.Index, entry.TermIndex.Term, entry.Content)

    let increment context (entry : Entry) =
        if context.NextIndex <> entry.TermIndex.Index then
            failwith "this should never happen"
        add context entry

    let incrementState entry state =
        { state with Log = increment state.Log entry }

    let addToState entry state =
        { state with Log = add state.Log entry }

    let append (context : LogContext) =
        List.fold add context

    let truncate (context : LogContext) index =
        let remove m =
            [index + 1 .. context.NextIndex]
            |> List.fold (flip Map.remove) m
        { context with
            NextIndex = index + 1
            Index = remove context.Index } 
        
    let appendOrTruncate context index =
        function
        | [] ->
            truncate context index
        | entries ->
            append context entries 

    let private createEntry term index (o : obj) =
        { TermIndex = TermIndex.create term index
          Content = o :?> LogContent }

    let tryGet context index =
        tryGet context index
        |> Option.map (fun (index,term,o) -> createEntry term index o) 

    let query context q =
        query context q
        |> Seq.map (fun (index, term, o) -> createEntry term index o)

    let length (context : LogContext) =
        context.NextIndex - 1 // TODO check this is right.

    let termIndex (context : LogContext) index =
        if index < 0 then failwith "argh"
        match Map.tryFind index context.Index with
        | Some (t, _) ->
            { Term = t; Index = index }
        | None ->
            TermIndex.Default

    let lastTermIndex (context : LogContext) =
        termIndex context (context.NextIndex - 1)

    let hasEntryFromTerm (context : LogContext) term =
        context.Index
        |> Map.tryFindKey (fun _ (t, _) -> t = term)
        |> Option.isSome

    let range context start finish =
        query context (Range(start, finish))

    let isConsistent context (termIndex : TermIndex) =
        match Map.tryFind termIndex.Index context.Index with
        | Some (term, _) -> term = termIndex.Term
        | _ -> termIndex = TermIndex.Default

    let create state x =
        { TermIndex = TermIndex.create state.Term.Current state.Log.NextIndex
          Content = x }

[<AutoOpen>]
module Lenses =

    open FSharpx.Lens.Operators

    let config =
        { Get = fun (state : RaftState) -> state.Config
          Set = fun config state -> { state with Config = config } }

    let log =
        { Get = fun (state : RaftState) -> state.Log
          Set = fun log state -> { state with Log = log } }

    let commitIndex =
        { Get = fun (state : RaftState) -> state.CommitIndex
          Set = fun ci state -> { state with CommitIndex = ci } }

    let currentTerm =
        { Get = fun (state : RaftState) -> state.Term.Current
          Set = fun ct state -> state.Term.Current <- ct; state  }

    let leader =
        { Get = fun (state : RaftState) -> state.Leader
          Set = fun leader state -> { state with Leader = leader } }

    let peers = 
        { Get = function
                | Normal n -> n.Peers
                | Joint (o, n) -> Map.merge n.Peers o.Peers
          Set = fun peers config -> 
            match config with
            | Normal n -> Normal { Peers = Map.update n.Peers peers }
            | Joint (o, n) ->
                Joint ({ Peers = Map.update o.Peers peers }, { Peers = Map.update n.Peers peers }) }

    let configPeers = config >>| peers

    let configPeer id = configPeers >>| Lens.forMap id

    let nextIndex = 
        { Get = fun peer -> Option.map (fun p -> p.NextIndex) peer
          Set = fun index peer -> 
            Option.map (fun p -> { p with NextIndex = (Option.getOrElse p.NextIndex index) }) peer }

    let matchIndex = 
        { Get = fun (peer : Peer option) -> Option.map (fun p -> p.MatchIndex) peer
          Set = fun mi peer -> 
            Option.map (fun p -> { p with MatchIndex = Option.getOrElse p.MatchIndex mi }) peer }

    let configPeerNextIndex id = configPeers >>| Lens.forMap id >>| nextIndex

    let configPeerMatchIndex id = configPeers >>| Lens.forMap id >>| matchIndex

    let peerModifier peerId nextIndexUpdater matchIndexUpdater =
        state {
            do! Lens.updateState (configPeerNextIndex peerId) nextIndexUpdater
            do! Lens.updateState (configPeerMatchIndex peerId) matchIndexUpdater }// (fun _ -> Some lastEntryIndex) }
            

[<AutoOpen>]
module Messages =

    open System

    type AppendEntriesRpcData = 
        { Term : int64
          LeaderId : Endpoint
          PrevLogTermIndex : TermIndex
          Entries : Entry list
          LeaderCommit : int }

    type AppendEntriesResultData =
        { Term : int64
          Success : bool
          LastEntryTermIndex : TermIndex }

    type RequestVoteRpcData =
        { Term : int64
          CandidateId : Guid
          LastLogTermIndex : TermIndex }

    type VoteResultData =
        { Term : int64
          VoteGranted : bool }

    type RaftProtocol =
        | AppendEntriesRpc of AppendEntriesRpcData 
        | AppendEntriesResult of AppendEntriesResultData
        | RequestVoteRpc of RequestVoteRpcData 
        | VoteResult of VoteResultData
        | AddPeer of Endpoint
        | RemovePeer of Endpoint
        | ClientCommand of byte[]
        | Ping
        | Pong
        | RpcFail
        | Exit  


[<AutoOpen>]
module Events =

    type ClusterChange =
        | Joined of Endpoint
        | Left of Endpoint
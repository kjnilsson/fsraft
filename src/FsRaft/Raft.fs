namespace FsRaft

open System
open System.IO

open FSharpx
open FSharpx.State
open FSharpx.Lens.Operators

[<AutoOpen>]
module Raft =

    open Messages
    open Persistence

    type RaftConfig =
        { Id : Guid
          Send : Endpoint -> byte[] -> Async<byte[]> //from, to, message
          Register : unit -> unit
          LogStream : Stream 
          TermStream : Stream }

    let pickler = new FsPickler.FsPickler()

    let deserialize (x:byte[]) =
        use s = new IO.MemoryStream(x)
        pickler.Deserialize<RaftProtocol> s

    let serialize (x: RaftProtocol) =
        use s = new MemoryStream()
        pickler.Serialize (s, x)
        s.ToArray()

    type internal Logger = Event<RaftLogEntry>

    let random = Random ()

    let electionTimout () = random.Next (RaftConstants.electionTimeoutFrom, RaftConstants.electionTimeoutTo)

    let applyConfigFollower config state =
        { state with Config = config }

    let applyConfigLeader log config state =
        match config, state.Config with
        | Joint (_, n), Joint (_, n') when Map.keys n.Peers = Map.keys n'.Peers -> // only add a log if receiving the log for the actual current configuration
            log (Debug (sprintf "raft :: %s leader: switching to Normal configuration" (short state.Id) ))
            let entry = Log.create state (ConfigEntry (Normal n'))
            Lens.set (Normal n') state Lenses.config // once joint config has been replicated immediately switch to new config - consensus isn't required.
            |> Lens.update (fun context -> Log.increment context entry) Lenses.log
        | _ -> state

    let internal applyLogs log isLeader applyConfig applyCommand commitIndex state =
        // this could potentially be slow for a leader if replication is much further
        // ahead of the commit index
        // we need FROM query as we need to apply cluster config ahead of the commit index
        let entries = Log.query state.Log (From <| state.CommitIndex + 1) |> Seq.toList
        entries
        |> List.fold (fun s e ->
            match e.Content with
            | Command cmd when e.TermIndex.Index <= commitIndex -> 
                if e.TermIndex.Index <= state.CommitIndex then failwith "noooo"
                #if DEBUG
                log (Debug (sprintf "raft :: %s applying command for index: %i, commitIndex: %i" (short state.Id) e.TermIndex.Index commitIndex))
                #endif
                applyCommand isLeader cmd
                { s with CommitIndex = e.TermIndex.Index }
            | Command _ -> s
            | ConfigEntry config -> 
                #if DEBUG
                log (Debug (sprintf "raft :: %s applying config for index: %i, commitIndex: %i" (short state.Id) e.TermIndex.Index commitIndex))
                #endif
                let newCommitIndex = min commitIndex e.TermIndex.Index
                { applyConfig config s with CommitIndex = newCommitIndex }) state

    let applyLogsFollower log = applyLogs log false applyConfigFollower
    let applyLogsLeader log = applyLogs log true (applyConfigLeader log)
    
    let internal forwardCommitIndex state = 
        match Log.length state.Log with
        | 0 -> state.CommitIndex
        | logLength ->
            Lens.get state Lenses.configPeers 
            |> Map.map (fun k v -> if k = state.Id then logLength else v.MatchIndex) // the leader wont update it's own LastAgree index
            |> Map.values
            |> medianInt
            |> max state.CommitIndex

    // leader log property
    let internal hasEntryFromCurrentTerm state =
        Log.hasEntryFromTerm state.Log state.Term.Current

    // evaluates the current state and applies any logs that have reached consensus.
    let rec evaluate log applyCommand state =
        let proposed = forwardCommitIndex state

        if hasEntryFromCurrentTerm state then 
            let state' =
                applyLogsLeader log applyCommand proposed state
                
            match state.Config, state'.Config with
            | Joint (_,_), Normal _ -> // only evaluate recursively if config has changed in this direction
                evaluate log applyCommand state'
            | _ -> state'
        else
            state

    let castVote currentTerm (lastLogTermIndex : TermIndex) (theirLastLogTermIndex : TermIndex) =
        match theirLastLogTermIndex, lastLogTermIndex with
        | r, l when r.Term > l.Term && r.Index >= l.Index -> // last entryerm of requester is higher - grant vote
            Some { Term = currentTerm; VoteGranted = true }
        | r, l when r.Term = l.Term && r.Index >= l.Index -> // requesters last index is higher - grant vote
            Some { Term = currentTerm; VoteGranted = true }
        | _ ->
            None

    let addPeer log (changes : Event<ClusterChange>) (state : RaftState) peerId =
        match state.Config with
        | Normal n when not (Map.containsKey peerId n.Peers) ->
            changes.Trigger (Joined peerId)
            let config = 
                let peer = 
                    { Peer.create() with 
                        NextIndex = (Log.lastTermIndex state.Log).Index }
                Joint (n, { n with Peers = Map.add peerId peer n.Peers } )
            let context =
                Log.create state (ConfigEntry config)
                |> Log.increment state.Log
            { state with 
                Config = config 
                Log = context }
        | _ ->
            log (Warn (sprintf "raft :: %s leader: in joint mode or peer already exists - can't add peer: %s" (short state.Id) (short peerId)))
            state        

    let containsPeer (state : RaftState) peerId =
        let allPeers = Lenses.configPeers |> Lens.get state
        Map.containsKey peerId allPeers

    let removePeer log (changes : Event<ClusterChange>) (state : RaftState) peerId =
        match state.Config with
        | Normal n when (Map.containsKey peerId n.Peers) ->
            changes.Trigger (Left peerId)
            let config = Joint (n, { n with Peers = Map.remove peerId n.Peers } )
            let context =
                Log.create state (ConfigEntry config)
                |> Log.increment state.Log
            { state with 
                Config = config 
                Log = context }
        | _ ->
            log (Warn (sprintf "raft :: %s leader: in joint mode or peer already exists - can't remove peer: %s" (short state.Id) (short peerId)))
            state        


    type internal StateMachineInteract<'TState> =
        | Apply of bool * byte[]
        | Restore of 'TState
        | Get of AsyncReplyChannel<'TState>

    type internal StateMachine<'TState when 'TState : equality> (apply, initialState) =
        let changed = Event<byte[] * bool * 'TState> ()
        let agent = MailboxProcessor.Start (fun inbox ->
            let rec loop state = async {
                let! msg = inbox.Receive ()
                match msg with
                | Apply (leader, o) ->
                    let state' = apply o state
                    changed.Trigger (o, leader, state')
                    return! loop state' 
                | Restore state ->
                    return! loop state
                | Get rc ->
                    rc.Reply state
                    return! loop state }
            loop initialState)
        do agent.Error.Add raise
        member this.Post = agent.Post
        member this.Get () = 
            agent.PostAndReply (fun rc -> Get rc)
        member this.Changes = changed.Publish


    type RaftAgent<'TState when 'TState : equality> (config : RaftConfig, initialState, apply) =
        let id = config.Id
        let shortId = (short id)
        let send peer (msg : RaftProtocol) = 
            async {
                let! response = config.Send peer (serialize msg) 
                return response |> deserialize }
        // logging
        let logger = Event<RaftLogEntry>()
        let debug msg = debug "raft" logger msg
        let warn msg = warn "raft" logger msg

        do debug "%s pending: started" shortId

        let result (aer : AppendEntriesRpcData) result (state : RaftState) =
            AppendEntriesResult
                { Term = state.Term.Current
                  Success = result 
                  LastEntryTermIndex = 
                    if result then 
                        Log.lastTermIndex state.Log 
                    else aer.PrevLogTermIndex }

        let started = Event<unit> ()
        let clusterChanges = Event<ClusterChange> ()

        let stateMachine = StateMachine (apply, initialState)
        let stateApply = fun isLeader c -> stateMachine.Post (Apply (isLeader, c))

        let changes = Event<byte[] * bool * 'TState> ()

        let setTerm term state =
            Lens.set term state Lenses.currentTerm

        let setTermVotedFor (state : RaftState) term votedFor =
            state.Term.Current <- term
            state.Term.VotedFor <- Some votedFor
            state

        let queueLen = Event<int>()
        let agent = MailboxProcessor<Guid * RaftProtocol * AsyncReplyChannel<RaftProtocol> option>.Start (fun inbox ->

            let receive () = 
                queueLen.Trigger inbox.CurrentQueueLength
                inbox.Receive ()

            let tryReceive t =
                queueLen.Trigger inbox.CurrentQueueLength
                inbox.TryReceive t
        
            let rec lead state = async {
                let heartbeat = new HeartbeatSuper (id, send, (fun e rp -> inbox.Post(e, rp, None)), state, logger.Trigger) // a use statement won't automatically call Dispose due to the mutual recursion.
                let! t = Async.CancellationToken
                t.Register (fun () -> dispose heartbeat) |> ignore

                // this only here for the startup dance
                Option.iterNone 
                    (fun _ -> inbox.PostAndAsyncReply(fun rc -> id, AddPeer id, Some rc) |> Async.Ignore |> Async.Start ) 
                    (Lenses.configPeer id |> Lens.get state)
                
                //update next and match index for all peers
                let state =
                    Lenses.configPeers |> Lens.get state
                    |> Map.fold (fun s k _ ->
                        exec (Lenses.peerModifier k (fun _ -> Some (Log.lastTermIndex state.Log).Index) (fun _ -> Some 0)) s) state

                let follow = fun s ->
                    dispose heartbeat
                    follow s

                let rec inner state = async {

                    let state = 
                        evaluate logger.Trigger stateApply state
                        |> heartbeat.State

                    let! from, msg, rc = receive ()
                    match msg with
                    | Exit -> 
                        dispose heartbeat
                        Option.iter (fun (rc : AsyncReplyChannel<RaftProtocol>) -> rc.Reply Exit) rc
                        debug "exiting"
                    | AppendEntriesRpc aer when aer.Term >= state.Term.Current ->
                        debug "%s leader: AppendEntriesRpc received with greater or equal term %i from %A - stepping down" shortId aer.Term from
                        inbox.Post(from, msg, rc) // self post and handle as follower
                        return! follow (setTerm aer.Term state)

                    | AppendEntriesResult res when not res.Success && res.Term > state.Term.Current ->
                        debug "%s leader: AppendEntriesResult with greater Term %i received from %A - stepping down" shortId res.Term from
                        inbox.Post(from, msg, rc) // self post and handle as follower
                        return! follow (setTerm res.Term state)
                        
                    | AppendEntriesResult res when not res.Success -> // decrement next index for peer
                        debug "%s leader: append entries not successful - decrementing next index for peer: %s" shortId (short from) 
                        let update =
                            Lenses.configPeer from
                            |> Lens.update (Option.map
                                (fun p -> { p with NextIndex = max p.MatchIndex (max 1 (p.NextIndex - 10)) })) // invariant - never go below match index or 0??
                        return! inner (update state)

                    | AppendEntriesResult res when res.Success ->
                        debug "%s leader: append entries success from %s" shortId (short from)
                        assert (res.LastEntryTermIndex.Index < state.Log.NextIndex)

                        // TODO: need to review and test this logic more
                        // what would you do if you received a AER with witha  LastEntryTermIndex higher than your current index?
                        let update =
                            Lenses.configPeer from
                            |> Lens.update (Option.map 
                                (fun p -> { p with NextIndex = res.LastEntryTermIndex.Index + 1 
                                                   MatchIndex = res.LastEntryTermIndex.Index }))

                        return! inner (update state) 
                    
                    | RequestVoteRpc rvr when not <| containsPeer state from ->
                        debug "%s leader: RequestVoteRpc received from unknown peer: %s" shortId (short from)
                        return! inner state

                    | RequestVoteRpc rvr when rvr.Term > state.Term.Current || state.Term.VotedFor = None ->
                        match castVote rvr.Term (Log.lastTermIndex state.Log) rvr.LastLogTermIndex with
                        | Some reply -> 
                            debug "%s leader: RequestVoteRpc received with greater term %i > %i - stepping down" shortId rvr.Term state.Term.Current
                            debug "%s leader: RequestVoteRpc voted for: %s in term: %i" shortId (short from) rvr.Term
                            Option.iter (fun (rc: AsyncReplyChannel<RaftProtocol>) -> rc.Reply (VoteResult reply)) rc
                            let state' = setTermVotedFor state rvr.Term from
                            return! follow state'
                        | None -> 
                            debug "%s leader: RequestVoteRpc received with greater term %i > %i - stepping down" shortId rvr.Term state.Term.Current 
                            //TODO self post
                            return! follow <| setTerm rvr.Term state 

                    | AddPeer peerId ->
                        debug "%s leader: AddPeer %s" shortId (short peerId)
                        return! inner (addPeer logger.Trigger clusterChanges state peerId)

                    | RemovePeer peerId ->
                        return! inner (removePeer logger.Trigger clusterChanges state peerId)

                    | ClientCommand cmd ->
                        debug "%s leader: client command: %s received from: %s" shortId (typeName msg) (short from)
                        let e = 
                            { TermIndex = TermIndex.create state.Term.Current (state.Log.NextIndex)
                              Content = Command cmd }
                        return! inner (Log.incrementState e state)

                    | _ ->
                        debug "%s leader: unmatched command: %s received from: %s" shortId (typeName msg) (short from)
                        return! inner state }
                return! inner state }

            and candidate (state : RaftState) = async {

                let peers = Lenses.configPeers |> Lens.get state
                let peersExcept = peers |> Map.filter (fun k _ -> k <> state.Id)

                // pinging at least one other node to ensure liveliness
                // slow running nodes should not initiate unnecessary elections
//                if peersExcept.Count > 0 then
//                    debug "%s candidate: pinging other nodes" shortId
//                    peersExcept
//                    |> Map.map (fun _ _ -> Ping)
//                    |> Map.iter send 
//
//                    let! pong = tryReceive 2000
//                    match pong with
//                    | Some (f, o) -> 
//                        match o with
//                        | Pong -> 
//                            debug "%s candidate: Pong received - proceeding with election..." shortId
//                        | _ -> 
//                            inbox.Post (f, o) // return message to queue in case it is a vote request
//                            return! follow state 
//                    | _ -> return! follow state

                let nextTerm = state.Term.Current + 1L
                let state = setTermVotedFor { state with Leader = None } nextTerm state.Id
                warn "%s candidate: initiating election for term: %i" shortId state.Term.Current

                peersExcept
                |> Map.map (fun _ _ ->
                    RequestVoteRpc
                        { Term = state.Term.Current
                          CandidateId = state.Id
                          LastLogTermIndex = Log.lastTermIndex state.Log }) 
                |> Map.map (fun k v ->
                    async {
                        let! resp = send k v
                        inbox.Post (k, resp, None)
                         })
                |> Map.toSeq
                |> Seq.map snd
                |> Async.Parallel
                |> Async.Ignore
                |> Async.Start

                let quorum = 1 + (peers.Count / 2)
                
                let rec inner votes = async {
                    if votes >= quorum then 
                        warn "%s candidate: quorum achieved - becoming leader of term: %i" shortId state.Term.Current
                        return! lead state
                    
                    let! msg = tryReceive (electionTimout ())
                    match msg with
                    | Some (from, msg, Some rc) ->
                        match msg with
                        | Exit -> 
                            printfn "exiting"
                            rc.Reply Exit
                        | AppendEntriesRpc aer when aer.Term >= state.Term.Current ->
                            debug "%s candidate: AppendEntriesRpc received with greater or equal term %i >= %i - candidate withdrawing" shortId aer.Term state.Term.Current
                            inbox.Post(from, msg, Some rc) //self post
                            return! follow <| setTerm aer.Term state

                        | AppendEntriesRpc aer -> // ensure leader with lower term steps down
                            let res = result aer false state
                            rc.Reply res
                            return! inner votes

                        | RequestVoteRpc rvr when not <| containsPeer state from ->
                            debug "%s candidate: RequestVoteRpc received from unknown peer: %s" shortId (short from)
                            //TODO figure out what to do here - reply or ignore?
                            return! candidate state

                        | RequestVoteRpc rvr when rvr.Term > state.Term.Current || state.Term.VotedFor = None ->
                            match castVote rvr.Term (Log.lastTermIndex state.Log) rvr.LastLogTermIndex with
                            | Some reply -> 
                                debug "%s candidate: RequestVoteRpc voted for: %s in term: %i" shortId (short from) rvr.Term
                                rc.Reply (VoteResult reply)
                                return! follow (setTermVotedFor state rvr.Term from)
                            | None ->
                                // TODO should we really ignore here? propbably
                                //rc.Reply (VoteResult { Term = state.Term.Current; VoteGranted = false })
                                return! follow (setTerm rvr.Term state) 
                        | _ -> return! candidate state
                    | Some (from, msg, None) ->
                        match msg with
                        | VoteResult vr when vr.Term > state.Term.Current ->
                            debug "%s candidate: VoteResult received with greater term: %i from: %s - candidate withdrawing" shortId vr.Term (short from)
                            return! follow <| setTerm vr.Term state

                        | VoteResult vr when vr.VoteGranted && vr.Term = state.Term.Current ->
                            return! inner (votes + 1)

                        | ClientCommand cmd ->
                            warn "%s candidate: client command: %s received and will be lost\r\n%A" shortId (typeName msg) msg
                            return! inner votes
                        | _ -> 
                            debug "%s candidate: unmatched command: %s received from: %s" shortId (typeName msg) (short from)
                            return! inner votes
                    | None -> return! candidate state }
                return! inner 1 }

            and follow (state : RaftState) = async {
                let! msg = tryReceive (electionTimout ())
                match msg with
                | Some (from, msg, rc) ->
                    match msg with
                    | Exit -> 
                        printfn "exiting"
                        Option.iter (fun (rc : AsyncReplyChannel<RaftProtocol>) -> rc.Reply Exit) rc
                    | AppendEntriesRpc aer when aer.Term < state.Term.Current ->
                        debug "%s follower: AppendEntriesRpc received with lower term: %i current term: %i" shortId aer.Term state.Term.Current 
                        match rc with
                        | Some rc -> result aer false state |> rc.Reply
                        | None -> ()
                        return! follow state

                    | AppendEntriesRpc aer when not (Log.isConsistent state.Log aer.PrevLogTermIndex) ->
                        debug "%s follower: AppendEntriesRpc received with inconsistent log from: %s request previous termindex: %A" shortId (short from) (aer.PrevLogTermIndex)
                        match rc with
                        | Some rc -> 
                            debug "%s follower: AppendEntriesRpc replying to %s" shortId (short from)
                            result aer false state |> rc.Reply
                        | None -> ()
                        return! follow state

                    | AppendEntriesRpc aer ->
                        let entries = aer.Entries |> List.filter (fun x -> x.TermIndex.Index > state.CommitIndex)
                        #if DEBUG
                        if entries.Length > 0 then debug "%s follower: AppendEntriesRpc %i logs received" shortId entries.Length
                        #endif
                        let state' =
                            { state with
                                Leader = Some aer.LeaderId
                                Log = Log.appendOrTruncate state.Log (max state.CommitIndex aer.PrevLogTermIndex.Index) entries }
                            |> setTerm aer.Term
                            |> applyLogsFollower logger.Trigger stateApply aer.LeaderCommit 

                        match rc with
                        | Some rc ->
                            result aer true state' |> rc.Reply
                        | None -> ()

                        assert (aer.PrevLogTermIndex.Index + aer.Entries.Length >= state'.Log.Index.Count)
                        return! follow state'
                    
                    | RequestVoteRpc rvr when not (containsPeer state from) ->
                        debug "%s follower: RequestVoteRpc received from unknown peer: %s" shortId (short from)
                        return! follow state
                         
                    | RequestVoteRpc rvr when rvr.Term < state.Term.Current ->
                        debug "%s follower: RequestVoteRpc received from: %s with lower term: %i, CurrentTerm: %i" shortId (short from) rvr.Term state.Term.Current
                        match rc with
                        | Some rc ->
                            rc.Reply (VoteResult { VoteGranted = false; Term = state.Term.Current })
                        | None -> ()
                        return! follow state

                    | RequestVoteRpc rvr when rvr.Term > state.Term.Current || state.Term.VotedFor = None ->
                        match castVote rvr.Term (Log.lastTermIndex state.Log) rvr.LastLogTermIndex with
                        | Some vote -> 
                            match rc with
                            | Some rc ->
                                rc.Reply (VoteResult vote)
                            | None -> ()
                            debug "%s follower: RequestVoteRpc voted for: %s in term: %i" shortId (short from) rvr.Term
                            return! follow (setTermVotedFor state rvr.Term from)
                        | None ->
                            return! follow (setTerm rvr.Term state)

                    | ClientCommand _ | AddPeer _ | RemovePeer _ ->
                        match state.Leader with
                        | Some leader ->
                            debug "%s follower: forwarding command: %s to %s" shortId (typeName msg) (short leader)
                            let! response = send leader msg
                            ()
                        | None -> ()
                        return! follow state

                    | Ping ->
                        debug "%s follower - ping received from %s returning pong" shortId (short from)
                        match rc with
                        | Some rc ->
                            rc.Reply Pong
                        | None -> ()
                        return! follow state

                    | _ -> 
                        debug "%s follower: unmatched command: %s received from: %s" shortId (typeName msg) (short from)
                        return! follow state
                | None -> 
                    return! candidate state }

            and wait () = async {
                let! msg = tryReceive 3000
                match msg with
                | None ->
                    debug "%s pending: requesting to join cluster" shortId
                    config.Register ()
                    return! wait ()
                | Some (_, m, _) ->
                    match m with
                    | AppendEntriesRpc aer ->
                        debug "%s pending: AppendEntriesRpc start message received" shortId
                        let logContext = makeContext config.LogStream
                        let termContext = Persistence.TermContext config.TermStream
                        let s =
                            RaftState.create id logContext termContext
                            |> applyLogsFollower logger.Trigger stateApply aer.LeaderCommit  

                        let _ = Observable.awaitPause stateMachine.Changes 2000.0
                        debug "%s pending: finished applying stored state - current commit index: %i - leader: %i" shortId s.CommitIndex aer.LeaderCommit
                        stateMachine.Changes.Add changes.Trigger
                        started.Trigger ()
                        changes.Trigger ([||], false, stateMachine.Get())
                        inbox.Post msg.Value
                        return! follow s
                    | _ -> 
                        debug "%s pending: other start message received" shortId
                        let logContext = makeContext config.LogStream
                        let termContext = Persistence.TermContext config.TermStream
                        if logContext.NextIndex > 1 then 
                            failwith "%s pending: Not Good! - starting node with previous logs without a commit index can be fatal" shortId
                        stateMachine.Changes.Add changes.Trigger
                        started.Trigger ()
                        return! follow (RaftState.create id logContext termContext) }
            wait () )

        //let subscriber = config.Receive |> Observable.subscribe agent.Post

        member this.PostAndAsyncReply (f, data) = 
            async {
                let! response = agent.PostAndAsyncReply (fun rc -> f, data, Some rc)
                return (response) }
        member this.Post cmd = agent.Post (id, cmd, None)
        member this.AddPeer id = agent.Post (id, AddPeer id, None)        
        member this.RemovePeer id = agent.Post (id, RemovePeer id, None)        

        member this.Changes = changes.Publish
        member this.Started = started.Publish
        member this.QueueLength = queueLen.Publish
        member this.ClusterChanges = clusterChanges.Publish

        member this.LogEntry = logger.Publish

        member this.Error = agent.Error

        // for testing
        member this.State = stateMachine.Get
        member internal this.Id = id 

        static member Start<'TState when 'TState : equality> (config : RaftConfig, initialState, apply) =
            new RaftAgent<'TState>(config, initialState, apply)

        interface IDisposable with
            member this.Dispose () = 
                //dispose subscriber
                this.PostAndAsyncReply (Guid.NewGuid(), Exit) |> Async.RunSynchronously |> ignore
                dispose agent
                dispose config.LogStream
                dispose config.TermStream
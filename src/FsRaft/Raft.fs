namespace FsRaft

open System
open System.IO
open FSharpx
open Nessos.FsPickler


module Application =
    open Persistence

    let shortEp (id, _, _) = short id

    let applyConfigFollower config state =
        { state with Config = config }

    let applyConfigLeader log config state =
        match config, state.Config with
        | Joint (_, n), Joint (_, n') when Map.keys n.Peers = Map.keys n'.Peers -> // only add a log if receiving the log for the actual current configuration
            log (Debug (sprintf "raft :: %s leader: switching to Normal configuration" (shortEp state.Id) ))
            let entry = Log.create state (ConfigEntry (Normal n'))
            Lens.set (Normal n') state Lenses.config // once joint config has been replicated immediately switch to new config - consensus isn't required.
            |> Lens.update (fun context -> Log.increment context entry) Lenses.log
        | _ -> state

    let applyLogs log isLeader applyConfig applyCommand commitIndex state =
        // this could potentially be slow for a leader if replication is much further
        // ahead of the commit index
        // we need FROM query as we need to apply cluster config ahead of the commit index
        let entries = Log.query state.Log (From <| state.CommitIndex + 1)
        entries
        |> Seq.fold (fun (s, changes) e ->
            match e.Content with
            | Command cmd when e.TermIndex.Index <= commitIndex -> 
                if e.TermIndex.Index <= s.CommitIndex then failwith "noooo"
                log (Debug (sprintf "raft :: %s applying command for index: %i, commitIndex: %i" (shortEp state.Id) e.TermIndex.Index commitIndex))
                let newState = applyCommand cmd s.State
                { s with
                    CommitIndex = e.TermIndex.Index
                    State = newState }, (cmd, isLeader, newState) :: changes
            | Command _ -> s, changes
            | ConfigEntry config -> 
                #if DEBUG
                log (Debug (sprintf "raft :: %s applying config for index: %i, commitIndex: %i" (shortEp state.Id) e.TermIndex.Index commitIndex))
                #endif
                let newCommitIndex = min commitIndex e.TermIndex.Index
                { applyConfig config s 
                    with CommitIndex = newCommitIndex }, changes) (state, [])

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
    let rec evaluate log applyCommand state changes =
        let proposed = forwardCommitIndex state

        if hasEntryFromCurrentTerm state then 
            let state', changes' =
                applyLogsLeader log applyCommand proposed state
            let changes = changes @ List.rev changes'
                
            match state.Config, state'.Config with
            | Joint (_,_), Normal _ -> // only evaluate recursively if config has changed in this direction
                evaluate log applyCommand state' changes
            | _ -> state', changes
        else
            state, changes

[<AutoOpen>]
module Raft =

    open Messages
    open Persistence
    open Application
    open Microsoft.FSharp.Reflection

    type Identifier = Rpc.Identifier

    type RaftConfig =
        { Id : Endpoint
          RpcFactory : ((Identifier -> byte[] -> Async<byte[]>) -> ((Identifier -> byte[] -> Async<byte[] option>) * IDisposable)) option
          Register : unit -> unit
          LogStream : Stream 
          TermStream : Stream }

    let pickler = new FsPickler()

    let deserialize (x:byte[]) =
        use s = new IO.MemoryStream(x)
        pickler.Deserialize<RaftProtocol> s

    let serialize (x: RaftProtocol) =
        use s = new MemoryStream()
        pickler.Serialize (s, x)
        s.ToArray()

    let shortEp (id, _, _) = short id

    let random = Random ()

    let inline typeName (x : RaftProtocol) = 
        match FSharpValue.GetUnionFields(x, typeof<RaftProtocol>) with
        | case, _ -> case.Name

    let electionTimout () = 
        random.Next (RaftConstants.electionTimeoutFrom, RaftConstants.electionTimeoutTo)

    let castVote currentTerm (lastLogTermIndex : TermIndex) (theirLastLogTermIndex : TermIndex) =
        match theirLastLogTermIndex, lastLogTermIndex with
        | r, l when r.Term > l.Term && r.Index >= l.Index -> // last entryerm of requester is higher - grant vote
            Some { Term = currentTerm; VoteGranted = true }
        | r, l when r.Term = l.Term && r.Index >= l.Index -> // requesters last index is higher - grant vote
            Some { Term = currentTerm; VoteGranted = true }
        | _ ->
            None

    let addPeer<'T> log (changes : Event<ClusterChange>) (state : RaftState<'T>) peerId =
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
            log (Warn (sprintf "raft :: %s leader: in joint mode or peer already exists - can't add peer: %s" (shortEp state.Id) (shortEp peerId)))
            state        

    let containsPeer<'T> (state : RaftState<'T>) peerId =
        let allPeers = Lenses.configPeers |> Lens.get state
        Map.containsKey peerId allPeers

    let removePeer<'T> log (changes : Event<ClusterChange>) (state : RaftState<'T>) peerId =
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
            log (Warn (sprintf "raft :: %s leader: in joint mode or peer already exists - can't remove peer: %s" (shortEp state.Id) (shortEp peerId)))
            state        

    type RaftAgent<'TState when 'TState : equality> (config : RaftConfig, initialState : 'TState, apply : byte[] -> 'TState -> 'TState ) =
        let id, _, _ = config.Id
        let ep = config.Id
        let shortId = (short id)
        // logging
        let logger = Event<RaftLogEntry>() // TODO replace with a logger that isn't synchronous
        let info msg = info "raft" logger msg
        let debug msg = debug "raft" logger msg
        let warn msg = warn "raft" logger msg

        let mutable clientState = Unchecked.defaultof<RaftState<'TState>>

        do debug "%s pending: started" shortId

        let result (aer : AppendEntriesRpcData) result (state : RaftState<'TState>) =
            AppendEntriesResult
                { Term = state.Term.Current
                  Success = result 
                  LastEntryTermIndex = 
                    if result then 
                        Log.lastTermIndex state.Log 
                    else aer.PrevLogTermIndex }

        let started = Event<unit> ()
        let becameLeader = Event<int64> ()
        let clusterChanges = Event<ClusterChange> ()
        let changes = Event<byte[] * bool * 'TState> ()

        let setTerm term state =
            Lens.set term state Lenses.currentTerm

        let setTermVotedFor (state : RaftState<'TState>) term votedFor =
            state.Term.Current <- term
            state.Term.VotedFor <- Some votedFor
            state

        let mutable requestorObj = null 

        let rpcFactory = 
            defaultArg 
                config.RpcFactory 
                (fun handleRequest ->
                    let r = new Rpc.DuplexRpcTcpListener(ep, handleRequest) 
                    (fun ident b -> r.Request(ident, b)), r :> IDisposable)
        
        let agent = MailboxProcessor<Endpoint * RaftProtocol * AsyncReplyChannel<RaftProtocol> option>.Start (fun inbox ->

            let processMsg ident data =
                async {
                    let p = deserialize data
                    let! result = inbox.PostAndTryAsyncReply  ((fun rc -> ident, p, Some rc), 500)
                    return 
                        match result with
                        | None -> RpcFail
                        | Some p -> p
                        |> serialize }

            let request, requestor = rpcFactory processMsg 
            requestorObj <- requestor//need this to ensure it gets disposed.

            let send peer (msg : RaftProtocol) = 
                async {
                    let m = serialize msg
                    let! response = request peer m
                    match response with
                    | Some res -> return deserialize res
                    | None -> return RpcFail }

            let receive () = inbox.Receive ()
            let tryReceive t = inbox.TryReceive t
        
            let rec lead state = async {
                let heartbeat = new HeartbeatSuper<'TState> (ep, send, (fun e rp -> inbox.Post(e, rp, None)), state, logger.Trigger) // a use statement won't automatically call Dispose due to the mutual recursion.
                let! t = Async.CancellationToken
                t.Register (fun () -> dispose heartbeat) |> ignore

                // this only here for the startup dance
                Option.iterNone 
                    (fun _ -> inbox.PostAndAsyncReply(fun rc -> ep, AddPeer ep, Some rc) |> Async.Ignore |> Async.Start ) 
                    (Lenses.configPeer ep |> Lens.get state)
                
                //update next and match index for all peers
                let state =
                    Lenses.configPeers 
                    |> Lens.get state
                    |> Map.fold (fun s k _ ->
                        State.exec (Lenses.peerModifier k (fun _ -> Some (Log.lastTermIndex state.Log).Index) (fun _ -> Some 0)) s) state

                let follow = fun s ->
                    dispose heartbeat
                    follow s
                
                becameLeader.Trigger state.Term.Current

                let rec inner state = async {

                    let state, newChanges = 
                        evaluate logger.Trigger apply state []

                    let state = heartbeat.State state
                    List.iter changes.Trigger newChanges
                    clientState <- state

                    let! fromEp, msg, rc = receive ()
                    let from, _, _  = fromEp
                    match msg with
                    | Exit -> 
                        dispose heartbeat
                        dispose state
                        Option.iter (fun (rc : AsyncReplyChannel<RaftProtocol>) -> rc.Reply Exit) rc
                        printfn "exiting"
                    | AppendEntriesRpc aer when aer.Term >= state.Term.Current ->
                        info "%s leader: AppendEntriesRpc received with greater or equal term %i from %O - stepping down" shortId aer.Term from
                        inbox.Post(fromEp, msg, rc) // self post and handle as follower
                        return! follow (setTerm aer.Term state)

                    | AppendEntriesResult res when not res.Success && res.Term > state.Term.Current ->
                        info "%s leader: AppendEntriesResult with greater Term %i received from %O - stepping down" shortId res.Term from
                        inbox.Post(fromEp, msg, rc) // self post and handle as follower
                        return! follow (setTerm res.Term state)
                        
                    | AppendEntriesResult res when not res.Success -> // decrement next index for peer
                        debug "%s leader: append entries not successful - decrementing next index for peer: %s lastentrytermindex: %i" shortId (short from) res.LastEntryTermIndex.Index
                        let update =
                            Lenses.configPeer fromEp
                            |> Lens.update (Option.map
                                (fun p -> { p with NextIndex = max p.MatchIndex (max 1 (p.NextIndex - 10)) })) // invariant - never go below match index or 0??
                        return! inner (update state)

                    | AppendEntriesResult res when res.Success ->
//                        debug "%s leader: append entries success from %s" shortId (short from)
                        assert (res.LastEntryTermIndex.Index < state.Log.NextIndex)

                        // TODO: need to review and test this logic more
                        // what would you do if you received a AER with witha  LastEntryTermIndex higher than your current index?
                        let update =
                            Lenses.configPeer fromEp
                            |> Lens.update (Option.map 
                                (fun p -> { p with NextIndex = res.LastEntryTermIndex.Index + 1 
                                                   MatchIndex = res.LastEntryTermIndex.Index }))

                        return! inner (update state) 
                    
                    | RequestVoteRpc rvr when not <| containsPeer state fromEp ->
                        debug "%s leader: RequestVoteRpc received from unknown peer: %s" shortId (short from)
                        return! inner state

                    | RequestVoteRpc rvr when rvr.Term > state.Term.Current || state.Term.VotedFor = None ->
                        match castVote rvr.Term (Log.lastTermIndex state.Log) rvr.LastLogTermIndex with
                        | Some reply -> 
                            info "%s leader: RequestVoteRpc received with greater term %i > %i - stepping down" shortId rvr.Term state.Term.Current
                            debug "%s leader: RequestVoteRpc voted for: %s in term: %i" shortId (short from) rvr.Term
                            Option.iter (fun (rc: AsyncReplyChannel<RaftProtocol>) -> rc.Reply (VoteResult reply)) rc
                            let state' = setTermVotedFor state rvr.Term from
                            return! follow state'
                        | None -> 
                            info "%s leader: RequestVoteRpc received with greater term %i > %i - stepping down" shortId rvr.Term state.Term.Current 
                            //TODO self post
                            return! follow <| setTerm rvr.Term state 

                    | AddPeer peerId ->
                        info "%s leader: AddPeer %A" shortId peerId
                        return! inner (addPeer logger.Trigger clusterChanges state peerId)

                    | RemovePeer peerId ->
                        return! inner (removePeer logger.Trigger clusterChanges state peerId)

                    | ClientCommand cmd ->
                        debug "%s leader: client command: %s received from: %s" shortId (typeName msg) (short from)
                        let e = 
                            { TermIndex = TermIndex.create state.Term.Current (state.Log.NextIndex)
                              Content = Command cmd }
                        match rc with
                        | Some rc -> rc.Reply Pong
                        | None -> ()
                        return! inner (Log.incrementState e state)

                    | _ ->
                        debug "%s leader: unmatched command: %s received from: %s" shortId (typeName msg) (short from)
                        return! inner state }
                return! inner state }

            and candidate (state : RaftState<'TState>) = async {

                let peers = Lenses.configPeers |> Lens.get state
                let peersExcept = peers |> Map.filter (fun k _ -> k <> state.Id)

                let stateId, _, _ = state.Id

                let nextTerm = state.Term.Current + 1L
                let state = setTermVotedFor { state with Leader = None } nextTerm stateId
                warn "%s candidate: initiating election for term: %i" shortId state.Term.Current
                //send out vote requests
                peersExcept
                |> Map.map (fun _ _ ->
                    RequestVoteRpc
                        { Term = state.Term.Current
                          CandidateId = stateId
                          LastLogTermIndex = Log.lastTermIndex state.Log }) 
                |> Map.map (fun k v ->
                    async {
                        let! resp = send k v
                        inbox.Post (k, resp, None) })
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
                    | Some (fromEp, msg, Some rc) ->
                        let from, _, _ = fromEp
                        match msg with
                        | Exit -> 
                            dispose state
                            printfn "exiting"
                            rc.Reply Exit

                        | AppendEntriesRpc aer when aer.Term >= state.Term.Current ->
                            debug "%s candidate: AppendEntriesRpc received with greater or equal term %i >= %i - candidate withdrawing" shortId aer.Term state.Term.Current
                            inbox.Post (fromEp, msg, Some rc) //self post
                            return! follow <| setTerm aer.Term state

                        | AppendEntriesRpc aer -> // ensure leader with lower term steps down
                            let res = result aer false state
                            rc.Reply res
                            return! inner votes

                        | RequestVoteRpc rvr when not <| containsPeer state fromEp ->
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
                        | _ -> 
                            warn "%s candidate: unmatched request: %s received %A" shortId (typeName msg) msg
                            return! candidate state
                    | Some ((from, _, _), msg, None) ->
                        match msg with
                        | VoteResult vr when vr.Term > state.Term.Current ->
                            debug "%s candidate: VoteResult received with greater term: %i from: %s - candidate withdrawing" shortId vr.Term (short from)
                            return! follow <| setTerm vr.Term state

                        | VoteResult vr when vr.VoteGranted && vr.Term = state.Term.Current ->
                            return! inner (votes + 1)

                        | ClientCommand _ ->
                            warn "%s candidate: client command: %s received and will be lost\r\n%A" shortId (typeName msg) msg
                            return! inner votes
                        | _ -> 
                            debug "%s candidate: unmatched reply: %s received from: %s" shortId (typeName msg) (short from)
                            return! inner votes
                    | None -> return! candidate state }
                return! inner 1 }

            and follow (state : RaftState<'TState>) = async {
                let! msg = tryReceive (electionTimout ())
                match msg with
                | Some ((from, _, _) as fromEp, msg, rc) ->
                    match msg with
                    | Exit -> 
                        printfn "exiting"
                        dispose state
                        Option.iter (fun (rc : AsyncReplyChannel<RaftProtocol>) -> rc.Reply Exit) rc

                    | AppendEntriesRpc aer when aer.Term < state.Term.Current ->
                        debug "%s follower: AppendEntriesRpc received with lower term: %i current term: %i" shortId aer.Term state.Term.Current 
                        match rc with
                        | Some rc -> result aer false state |> rc.Reply
                        | None -> ()
                        return! follow state

                    | AppendEntriesRpc aer when not (Log.isConsistent state.Log aer.PrevLogTermIndex) ->
                        debug "%s follower: AppendEntriesRpc received with inconsistent log from: %s request previous termindex: %A current commit %i" shortId (short from) (aer.PrevLogTermIndex.Term, aer.PrevLogTermIndex.Index) state.CommitIndex
                        match rc with
                        | Some rc -> 
                            debug "%s follower: AppendEntriesRpc replying to %s" shortId (short from)
                            result aer false state |> rc.Reply
                        | None -> ()
                        return! follow state

                    | AppendEntriesRpc aer ->
                        let logIndexCount = state.Log.Index.Count
                        let entries = aer.Entries |> List.filter (fun x -> x.TermIndex.Index > state.CommitIndex)
                        if entries.Length > 0 then debug "%s follower: AppendEntriesRpc %i logs received, request prev termindex: %A %s" shortId entries.Length (aer.PrevLogTermIndex.Term, aer.PrevLogTermIndex.Index) (state.show())
                        let state', newChanges =
                            { state with
                                Leader = Some aer.LeaderId
                                Log = Log.appendOrTruncate state.Log (max state.CommitIndex aer.PrevLogTermIndex.Index) entries }
                            |> setTerm aer.Term
                            |> applyLogsFollower logger.Trigger apply aer.LeaderCommit

                        List.iter changes.Trigger newChanges
                        clientState <- state'

                        match rc with
                        | Some rc ->
                            result aer true state' |> rc.Reply
                        | None -> ()

                        (*
                        if state.Log.Index.Count > state'.Log.Index.Count then
                            warn "%s follower: index count after AppendEntriesRpc decreased from %i to %i" shortId state.Log.Index.Count state'.Log.Index.Count

                        if not (aer.PrevLogTermIndex.Index + aer.Entries.Length >= state'.Log.Index.Count) then
                            warn "UGH: %s %A lic: %i after: %i new changes %i commitIndex %i entries: %i" shortId aer.Term logIndexCount (state'.Log.Index.Count) (newChanges.Length) state.CommitIndex entries.Length
                        else ()
                            *)
                        return! follow state'
                    
                    | RequestVoteRpc _ when not (containsPeer state fromEp) ->
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
                            debug "%s follower: forwarding command: %s to %A" shortId (typeName msg) (leader)
                            send leader msg |> Async.Ignore |> Async.Start
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
                        match rc with
                        | Some rc ->
                            rc.Reply RpcFail
                        | None -> ()
                        return! follow state
                | None -> 
                    return! candidate state }

            and wait () = async {
                debug "enter wait"
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
                        let termContext = new TermContext (config.TermStream)
                        let s, newChanges =
                            RaftState<'TState>.create ep initialState logContext termContext
                            |> applyLogsFollower logger.Trigger apply aer.LeaderCommit  

                        clientState <- s

                        List.iter changes.Trigger newChanges

                        debug "%s pending: finished applying stored state - current commit index: %i - leader: %i" shortId s.CommitIndex aer.LeaderCommit
                        started.Trigger ()
                        changes.Trigger ([||], false, initialState)
                        inbox.Post msg.Value
                        return! follow s
                    | _ -> 
                        debug "%s pending: other start message received" shortId
                        let logContext = makeContext config.LogStream
                        let termContext = new TermContext (config.TermStream)
                        if logContext.NextIndex > 1 then 
                            failwith "%s pending: Not Good! - starting node with previous logs without a commit index can be fatal" shortId
                        started.Trigger ()
                        return! follow (RaftState<'TState>.create ep initialState logContext termContext) }
            wait () )

        member __.PostAndAsyncReply (f, data) = 
            async {
                let! response = agent.PostAndAsyncReply (fun rc -> f, data, Some rc)
                return response }

        member __.Post cmd = agent.Post (ep, ClientCommand cmd, None)
        member __.AddPeer ep = agent.Post (ep, AddPeer ep, None)        
        member __.RemovePeer ep = agent.Post (ep, RemovePeer ep, None)        

        member __.Changes = Event.map (fun x -> x) changes.Publish
        member __.Started = started.Publish
        member __.BecameLeader = becameLeader.Publish

        member __.ClusterChanges = clusterChanges.Publish

        member __.LogEntry = logger.Publish

        member __.Error = agent.Error

        // for testing
        member __.State = clientState

        member __.Id = id 

        static member Start<'TState when 'TState : equality> (config : RaftConfig, initialState, apply) =
            new RaftAgent<'TState>(config, initialState, apply)

        interface IDisposable with
            member this.Dispose () = 
                dispose requestorObj
                this.PostAndAsyncReply ((Guid.NewGuid(), "", 0), Exit) |> Async.RunSynchronously |> ignore
                dispose agent

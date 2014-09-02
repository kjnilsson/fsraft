open System
open System.Threading
open System.IO
open FSharpx
open Nessos.FsPickler
open FsRaft
open FsRaft.Messages


type Network =
    | Register of Guid * (Endpoint -> byte [] -> Async<byte[]>)
    | Rpc of Endpoint * Endpoint * AsyncReplyChannel<Endpoint -> byte[] -> Async<byte[]>> //from to msg
    | Isolate
    | IsolateX of Guid
    | Heal
    | Shutdown of AsyncReplyChannel<unit>

let rand = 
    let random = Random()
    fun t -> random.Next(1, t)

let randomKey m =
    let keys = Map.keys m 
    let r = random.Next (0,keys.Length) 
    keys.[r]

let move x m1 m2 =
    match Map.tryFind x m1 with
    | Some xm1 -> Map.remove x m1, Map.add x xm1 m2
    | None -> m1, m2

let makeNetwork () =
    MailboxProcessor.Start(fun inbox -> 
        let rec loop (primary, secondary) = async {
            let! msg = inbox.Receive ()
            match msg with
            | Shutdown rc ->
                printfn "exiting network"
                rc.Reply ()
            | Register (i, rpc) ->
                return! loop <| (Map.add i (rpc) primary, secondary)
            | Rpc ((f,_,_), (t,_,_), rc) ->
                match Map.tryFind f primary, Map.tryFind t primary with
                | Some _, Some (rpc) -> 
                    rc.Reply rpc
                | _ -> 
                    match Map.tryFind f secondary, Map.tryFind t secondary with
                    | Some _, Some (rpc) -> 
                        rc.Reply rpc
                    | _ -> 
//                        printfn "\r\nno rpc endpoint %A %A" f t ; 
                        ()
                return! loop (primary, secondary)
            | Isolate ->
                let x = randomKey primary 
                printfn "Isolating: %A" (short x)
                return! loop (move x primary secondary)
            | IsolateX x ->
                printfn "Isolating: %A" (short x)
                return! loop (move x primary secondary)
            | Heal ->
                printfn "Healing network"
                return! loop (Map.merge primary secondary, Map.empty) }
        loop (Map.empty, Map.empty))

let pickler = new FsPickler()

let deserialize (x:byte[]) =
    use s = new IO.MemoryStream(x)
    pickler.Deserialize s

let serialize x =
    use s = new MemoryStream()
    pickler.Serialize (s, x)
    s.ToArray()

let call (network : MailboxProcessor<Network>) f t data = 
    async {
        let! rpc = network.PostAndAsyncReply (fun rc -> Rpc (f, t, rc))
        return! rpc f data }

type Calc =
    | Add of int
    | Div of int

type TestState = int * int * byte[] list

let apply (c : byte[]) (count, s, commands) =
    match deserialize c with
    | Add n -> count + 1, s + n, c :: commands
    | Div n -> count + 1, s / n, c :: commands

let create (network : MailboxProcessor<Network>) id port = 
    let ep = id, "127.0.0.1", port
    let config = 
        { Id = ep
//          Send = call network ep
          Send = (fun ep data -> 
            async {
                return [||] })
          Register = fun () -> ()
          LogStream = new MemoryStream()
          TermStream = new MemoryStream() }
     
    let agent = RaftAgent.Start<TestState> (config, (0,0,[]), apply)
    let rpc f data =
        async {
            let! result = agent.PostAndAsyncReply  (f, deserialize data)
            return serialize result }
//    network.Post (Register (id, rpc))
//    agent.LogEntry.Add (printfn "%A")
    agent, config
    
//let consoleLogger (entry : Entries.Entry) =
//    let now = System.DateTime.UtcNow.ToString ()
//    match entry.Level with
//    | Error -> printfn "%s - %s [%A] '%s' \r\nException: %A" now entry.Source.Category entry.Level entry.Data.Message entry.Data.Exception
//    | Warn -> printfn "%s - %s [%A] '%s'" now entry.Source.Category entry.Level entry.Data.Message
//    | _ -> () // 
//
//
//addLoggingTransport "console" { Transport.Send = consoleLogger }

let makeLeader id network port  =
    let l, config = create network id port
    l.Post (ClientCommand <| (serialize <| Add 1))
//    send network id id (ClientCommand <| (serialize <| Add 1))
    Threading.Thread.Sleep 2500 // allow leader time to become leader
    l, config
    

let createPeer silent (network : MailboxProcessor<Network>) (leader : RaftAgent<TestState>) port =
    let id = Guid.NewGuid()
    printfn "creating peer: %s" (short id)
    let p, config = create network id port
    if not silent then
        p.Changes.Add(fun _ -> printf ".")
    leader.AddPeer config.Id
    Async.AwaitEvent(p.Started) |> Async.RunSynchronously
    p, config

let randomOp () =
    if rand 10 < 4 then
        Div (rand 3)
    else
        Add (rand 1000)
    |> serialize

let validate (peers : RaftAgent<TestState> list) =
    let isValid =
        seq { yield! peers }
        |> Seq.map (fun x -> x.State())
        |> Seq.distinct
        |> Seq.length = 1
    if isValid = false then
        peers |> List.iter (fun x -> 
            let a, b, _ = x.State()
            printfn "State: %A %i %i" x.Id a b   )
    let _, s, _ = peers.Head.State()
    isValid, s 

let disposePeers (peers : RaftAgent<TestState> list) =
    peers |> List.iter dispose

let awaitEvent event timeOut =
    let are = ref (new AutoResetEvent false)
    use timer = new Timers.Timer (timeOut)
    timer.AutoReset <- false
    timer.Elapsed.Add (fun _ -> (!are).Set() |> ignore)
    use sub = Observable.subscribe(fun _ -> timer.Stop(); timer.Start()) event
    timer.Start()
    (!are).WaitOne()

let awaitPeers (peers : RaftAgent<TestState> list) =
    let x =
        peers
        |> List.map (fun x -> x.Changes) 
        |> List.reduce (fun s p -> Event.merge s p)// (peers.Head.Changes) 

    printfn "waiting for replication to finish"

    awaitEvent x 2000.0

let randomPeer (peers : RaftAgent<TestState> list) =
    peers.[random.Next(List.length peers)]
    
let basic silent =
    async {
        let network = makeNetwork()
        let leaderId = Guid.NewGuid()
//        let send = send network
        let leader, leaderConfig = makeLeader leaderId network 1236
        let peers = leader :: ([0..6] |> List.map (fun p -> fst <| createPeer silent network leader (4321 + p)))
        for x = 0 to 100 do
            do! Async.Sleep 50
            if not silent then printf "*"
            leader.Post (ClientCommand (randomOp()))
        awaitPeers peers |> ignore
        let isValid = validate peers
        network.PostAndReply (fun rc -> Shutdown rc)
        disposePeers peers
        return isValid } 

//let isolateSome silent =
//    async {
//        let network = makeNetwork()
//        let leaderId = Guid.NewGuid()
//        let leader,_ = makeLeader leaderId network 1237
//        let peers = leader :: ([0..2] |> List.map (fun p -> fst <| createPeer silent network leader (4330 + p)))
//        for x = 0 to 250 do
//            if x = 50 then network.Post Isolate
//            if x = 65 then network.Post (IsolateX leaderId)
//            if x = 165 then network.Post Isolate
//            if x = 190  then network.Post Heal
//            do! Async.Sleep 50
//            if not silent then printf "*"
//            leader.Post (ClientCommand (randomOp()))
//        awaitPeers peers |> ignore
//        let isValid = validate peers
//        network.PostAndReply (fun rc -> Shutdown rc)
//        disposePeers peers
//        return isValid } 
//
//let isolate2 silent =
//    async {
//        let network = makeNetwork()
//        let leaderId = Guid.NewGuid()
//        let leader,_ = makeLeader leaderId network
//        let peers = leader :: ([0..6] |> List.map (fun _ -> fst <| createPeer silent network leader))
//        for x = 0 to 250 do
//            if x = 50 then network.Post Isolate
//            if x = 55  then network.Post Isolate
//            if x = 95 then network.Post Heal
//            if x = 165 then network.Post Isolate
//            if x = 190  then network.Post Heal
//            do! Async.Sleep 35
//            if not silent then printf "*"
//            let p = randomPeer peers
//            leader.Post (ClientCommand (randomOp()))
//        awaitPeers peers |> ignore
//        let isValid = validate peers
//        network.PostAndReply (fun rc -> Shutdown rc)
//        disposePeers peers
//        return isValid } 
//
//let restore silent =
//    async {
//        let network = makeNetwork()
//        let leaderId = Guid.NewGuid()
//        let leader, config = makeLeader leaderId network
//        let peers = [leader]
//        for x = 0 to 50 do
//            do! Async.Sleep 25
//            if not silent then printf "*"
//            leader.Post (ClientCommand (randomOp()))
//        awaitPeers peers |> ignore
//        let events = Event<Guid * RaftProtocol>()
//        let test = new RaftAgent<TestState> (config, (0,0,[]), apply)
//        let aer = 
//            { Messages.AppendEntriesRpcData.Term = 1L
//              Messages.AppendEntriesRpcData.LeaderId = leaderId
//              Messages.AppendEntriesRpcData.PrevLogTermIndex = TermIndex.Default 
//              Messages.AppendEntriesRpcData.Entries = [] 
//              Messages.AppendEntriesRpcData.LeaderCommit = 101 } // extra margin here
//        events.Trigger (leaderId, AppendEntriesRpc aer)
//        awaitPeers [test] |> ignore
//        let isValid = validate [leader;test]
//        network.PostAndReply (fun rc -> Shutdown rc)
//        disposePeers peers
//        return isValid } 

[<EntryPoint>]
let main argv = 
    let silent = true
    Async.RunSynchronously (basic silent) |> printfn "basic is: %A"
//    Async.RunSynchronously (isolateSome silent) |> printfn "isolateSome is: %A"
//    Async.RunSynchronously (restore silent) |> printfn "restore is: %A"
    Console.ReadLine () |> ignore
    0
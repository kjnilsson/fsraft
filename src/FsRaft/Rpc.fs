module FsRaft.Rpc

open System
open System.IO
open System.Net
open System.Net.Sockets

let getBytes (s : string) = Text.Encoding.UTF8.GetBytes s

type Correlation = | Correlation of byte[]

let correlation () =
    let g = Guid.NewGuid()
    (string g).Substring(0, 7) 
    |> getBytes 
    |> Correlation

// Protocol
// [7 : correlation][1 : 0 = req, 1 = res][4 : data length][... data ...]

type Frame =
    | Request of Correlation * byte[] //correlation, data
    | Response of Correlation * byte[] //correlation, data

let writeFrame (stream: Stream) frame = 
    let write (Correlation corr) ft (data : byte[]) = 
        async { 
            do! stream.AsyncWrite corr
            do! stream.AsyncWrite [|ft|]
            do! stream.AsyncWrite (data.Length |> BitConverter.GetBytes)
            do! stream.AsyncWrite data }
        |> Async.Catch

    match frame with
    | Request (corr, data) -> write corr 0uy data
    | Response (corr, data) -> write corr 1uy data

let readFrame (stream : Stream) = 
    async {
        let! header = stream.AsyncRead 12
        let corr = header.[0 .. 6]
        let ft = header.[7]
        let dl = header.[8 .. 11]
        let dataLen = BitConverter.ToInt32(dl, 0)
        let! data = stream.AsyncRead dataLen
        match ft with
        | 0uy -> return Request (Correlation corr, data) 
        | 1uy -> return Response (Correlation corr, data)
        | _ -> return failwith "unexpected data type encountered" }
    |> Async.Catch

let readIdentifier (stream : Stream) =
    async {
        let! lenBuf = stream.AsyncRead 4
        let len = BitConverter.ToInt32 (lenBuf, 0)
        let! identData = stream.AsyncRead len
        let i = Text.Encoding.UTF8.GetString identData
        printfn "identifier string: %s" i
        match i.Split([|'@';':'|]) with
        | [|g;h;p|] -> return (Guid g), h, Int32.Parse p
        | _ -> return failwith "invalid identifier" }
    |> Async.Catch

type Identifier = Guid * string * int

let writeIdentifier (ident : Identifier) (s : Stream) =
    async {
        let i, h, p = ident
        let me = sprintf "%O@%s:%i" i h p 
        do! s.AsyncWrite (BitConverter.GetBytes me.Length)
        do! s.AsyncWrite (me |> getBytes)   }


type DuplexRpcProtocol =
    | Send of Correlation * byte[] * AsyncReplyChannel<byte[]>
    | Receive of Frame 
    | Abandon of Correlation

type DuplexRpcAgent (ident, client : TcpClient, getResponse) =
    let stream = client.GetStream()

    let writer = MailboxProcessor.Start (fun inbox ->
        let rec loop state = async {
            let! ft = inbox.Receive()
            let! result = writeFrame state ft
            
            return! loop state }
        loop stream) 

    let responder = MailboxProcessor.Start(fun inbox ->
        let rec loop state = async {
            let! corr, data = inbox.Receive()
            let! response = getResponse ident data
            writer.Post (Response (corr, response))
            return! loop state }
        loop [])

    let agent = MailboxProcessor.Start(fun inbox ->
        let rec loop state = async {
            let! msg = inbox.Receive()
            match msg with
            | Send (corr, data, rc) ->
                writer.Post (Request (corr, data))
                return! loop (Map.add corr rc state)
            | Receive (Request (corr, data)) ->
                responder.Post (corr, data)
                return! loop state                
            | Receive (Response (corr, data)) ->
                match Map.tryFind corr state with
                | Some rc ->
                    rc.Reply data
                    return! loop (Map.remove corr state) 
                | None ->
                    //printfn "unmatched response"
                    return! loop state 
             | Abandon corr ->
                return! loop (Map.remove corr state) }
        loop Map.empty)

    do async {
        while true do
            let! frame = readFrame stream
            match frame with
            | Choice1Of2 (f) ->
                agent.Post (Receive f)
            | _ -> printfn "error reading frame" }
    |> Async.Start

    member __.Send data =
        async {
            let corr = correlation()
            let! response = agent.PostAndTryAsyncReply ((fun rc -> Send (corr, data, rc)), 200)
            match response with
            | Some _ -> return response
            | None ->
                agent.Post (Abandon corr)
                return response }
             

    interface IDisposable with
        member __.Dispose () =
            stream.Dispose()
            (client :> IDisposable).Dispose()
    
    
type DuplexRpcListenerProtocol =
    | Get of Identifier * AsyncReplyChannel<byte[] -> Async<byte[] option>>
    | Add of Identifier * TcpClient

type DuplexRpcListener (identity : Identifier, getResponse : Identifier -> byte [] -> Async<byte []>) =
    do printfn "identify %A" identity
    let _, host, port = identity
    let listener = new TcpListener (IPAddress.Parse host, port)
    do listener.Start()
    do printfn "listening on %s:%i" host port

    let agent = MailboxProcessor.Start(fun inbox ->
        let rec loop state = async {
            let! msg = inbox.Receive()
            match msg with
            | Get (ident, rc) ->
                match Map.tryFind ident state with
                | Some (x : DuplexRpcAgent) -> 
                    rc.Reply (x.Send)
                    return! loop state
                | None ->
                    printfn "no current connection - connection to: %A" ident
                    let _, h, p = ident
                    let client = new TcpClient (h, p)
                    let s = client.GetStream()
                    do! writeIdentifier identity s
                    let! r = s.AsyncRead 2

                    //TODO handle ident response
                    let dra = new DuplexRpcAgent(ident, client, getResponse)
                    rc.Reply (dra.Send)
                    return! loop (Map.add ident dra state) 
             | Add (ident, client) -> 
                let dra = new DuplexRpcAgent(ident, client, getResponse)
                return! loop (Map.add ident dra state) }
        loop Map.empty)
                
    do async {
        while true do
            let! client = listener.AcceptTcpClientAsync() |> Async.AwaitTask
            printfn "new client connection"
            let stream = client.GetStream()
            let! ident = readIdentifier stream
            // TODO validate ident
            match ident with
            | Choice1Of2 (ident) ->
                agent.Post (Add (ident, client))
                do! stream.AsyncWrite "OK"B 
            | Choice2Of2 ex ->
                client.Close()
                printfn "error accepting client %A" ex } 
    |> Async.Start

    member __.Request (ident, data) = 
        async {
            let! f = agent.PostAndAsyncReply ((fun rc -> Get (ident, rc)))
            return! f data }
            
//let handle (b:byte[]) =
//    async { return Array.rev b }
//
//let oneIdent = (Guid.NewGuid(), "127.0.0.1", 1234)
//let one = DuplexRpcListener(oneIdent, handle)
//let twoIdent = (Guid.NewGuid(), "127.0.0.1", 4321)
//let two = DuplexRpcListener(twoIdent, handle)
//
//
//one.Request (twoIdent, "hello"B) |> Async.RunSynchronously |> printfn "%A"
//
//for i in [0..1000] do
//    two.Request (oneIdent, "world"B) |> Async.RunSynchronously |> printfn "%A"
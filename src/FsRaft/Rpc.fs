module FsRaft.Rpc

open System
open System.IO
open System.Net
open System.Net.Sockets
open System.Threading

let encode (s : string) = Text.Encoding.UTF8.GetBytes s
let decode (b : byte []) = Text.Encoding.UTF8.GetString b

type Correlation = Correlation of byte[]

type Identifier = Guid * string * int

let decodeCorr (Correlation c) =
    decode c

let correlation () =
    let g = Guid.NewGuid()
    (string g).Substring(0, 7) 
    |> encode 
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
//        printfn "identifier string: %s" i
        match i.Split([|'@';':'|]) with
        | [|g;h;p|] -> return (Guid g), h, Int32.Parse p
        | _ -> return failwith "invalid identifier" }
    |> Async.Catch

let writeIdentifier (ident : Identifier) (s : Stream) =
    async {
        let i, h, p = ident
        let me = sprintf "%O@%s:%i" i h p |> encode 
        do! s.AsyncWrite (BitConverter.GetBytes me.Length)
        do! s.AsyncWrite me }


type DuplexRpcProtocol =
    | Send of Correlation * byte[] * AsyncReplyChannel<byte[]>
    | Receive of Frame 
    | Abandon of Correlation

type DuplexRpcAgent (ident, client : TcpClient, getResponse, error) =
    let stream = client.GetStream()

    let writer = MailboxProcessor.Start (fun inbox ->
        let rec loop state = async {
            let! ft = inbox.Receive()
            let! _ = writeFrame state ft
            
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
                    printfn "unmatched response %s" (decodeCorr corr)
                    return! loop state 
             | Abandon corr ->
                printfn "abandoning.. %A %s" ident (decodeCorr corr)
                return! loop (Map.remove corr state) }
        loop Map.empty)

    let token = new CancellationTokenSource()
    do 
        async {
            let! t = Async.CancellationToken
            while not t.IsCancellationRequested do
                let! frame = readFrame stream
                match frame with
                | Choice1Of2 (f) ->
                    agent.Post (Receive f)
                | Choice2Of2 ex -> 
                    token.Cancel()
                    error()
                    printfn "%A error reading frame %A" ident ex }
        |> fun t -> Async.Start(t, token.Token)

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
            try
                token.Cancel()
                dispose token
                stream.Dispose()
                (client :> IDisposable).Dispose()
            with | _ -> ()
    
type DuplexRpcListenerProtocol =
    | Get of Identifier * AsyncReplyChannel<byte[] -> Async<byte[] option>>
    | Add of Identifier * TcpClient
    | Remove of Identifier
    | Exit of AsyncReplyChannel<unit>

type DuplexRpcTcpListener (identity : Identifier, getResponse : Identifier -> byte [] -> Async<byte []>) =
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
                    async {
                        try
                            let client = new TcpClient (h, p)
                            let s = client.GetStream()
                            do! writeIdentifier identity s
                            let! _ = s.AsyncRead 2
                            //TODO handle ident response
                            inbox.Post (Add(ident, client)) 
                            inbox.Post msg
                        with
                        | ex -> 
                            printfn "failed to connect to %A with %s" ident ex.Message
                            rc.Reply (fun _ -> async { return None }) }
                    |> Async.Start // need to process on background thread
                    return! loop state 
            | Add (ident, client) -> 
                let error = fun () -> inbox.Post (Remove ident)
                let dra = new DuplexRpcAgent(ident, client, getResponse, error)
                return! loop (Map.add ident dra state)
            | Remove ident ->
                let dra = Map.tryFind ident state
                dispose dra
                return! loop (Map.remove ident state)
            | Exit rc ->
                printfn "exiting TcpListener %A" identity
                Map.iter (fun _ v -> dispose v) state
                rc.Reply () }
                
        loop Map.empty)
    do agent.Error.Add raise
                
    do async {
        while true do
            let! client = listener.AcceptTcpClientAsync() |> Async.AwaitTask
            printfn "new client connection from %A" client.Client.RemoteEndPoint
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

    interface IDisposable with
        member __.Dispose() =
            try
                agent.PostAndReply (Exit, 1000)
                listener.Stop()
                dispose agent
            with | _ -> ()
   
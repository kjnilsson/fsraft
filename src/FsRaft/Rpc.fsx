#load "Rpc.fs"
open System
open FsRaft.Rpc

let (Correlation c) = correlation()
decode (Array.rev c            )
let handle (b:byte[]) =
    async { return Array.rev b }

let oneIdent = (Guid.NewGuid(), "127.0.0.1", 1234)
let one = DuplexRpcListener(oneIdent, handle)
let twoIdent = (Guid.NewGuid(), "127.0.0.1", 4321)
let two = DuplexRpcListener(twoIdent, handle)


one.Request (twoIdent, "hello"B) |> Async.RunSynchronously |> printfn "%A"

for i in [0..1000] do
    two.Request (oneIdent, "world"B) |> Async.RunSynchronously |> printfn "%A"

#r "/Users/karlnilsson/lib/FsCheck.dll"
#r "bin/Debug/FSharpx.Core.dll"
#r "bin/Debug/FsPickler.dll"


#load "Prelude.fs"

#load "Rpc.fs"

#load "Persistence.fs"
#load "Model.fs"
#load "Heartbeat.fs"
#load "Raft.fs"

open FsCheck

open FsRaft
open FsRaft.Model

Gen.choose(0, 10)
|> Gen.sample 10 10


Arb.generate<int64*int>
|> Gen.sample 10 10 

Raft.castVote










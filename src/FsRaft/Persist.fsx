#r "bin/Debug/FSharpx.Core.dll"
#r "bin/Debug/FsPickler.dll"

#load "Prelude.fs"
#load "Persistence.fs"
open FsRaft
#load "Model.fs"

open System
open System.IO
open Nessos.FsPickler
open FsRaft.Persistence

type Status =
    | Pending
    | Active
    | Stopped
    
type Instance =
    { Package : string * string option
      Dependencies : (string * string option) list
      RequirementId : string
      NodeId : Guid option
      Status : Status }
type Action =
    | Add
    | Update
    | Delete

type App =
    | Instance of Action * Guid * Instance

let instance =
    { Package = ("asdfasdfasdfasdfasdfasdfasdfasdfasdf", None)
      Dependencies = [0..10] |> List.map (fun x -> "asdfasdfasdfasdfasdfasdfwerqwerwqerqwerqwerqewrqwerqwerw", Some "asdfasdfqwer")
      RequirementId = "adsfasdfasdfasdfasdfasdfasdfasdfasdfasdfs"
      NodeId = Some (Guid.NewGuid ())
      Status = Pending }
let app = Instance (Add, Guid.NewGuid (), instance)

let fileStream = new FileStream(@"c:\dump\raft\test.dat", FileMode.OpenOrCreate,FileAccess.ReadWrite)

let context = makeContext fileStream

//tryGet context 8
let context' = 
    [0..10000]
    |> List.fold (fun state n -> 
        writeRecord state (state.NextIndex, 1L, app) ) context
//
fileStream.Flush()
fileStream.Dispose ()
//tryGet context' 10
let context'' = writeRecord context' (context'.NextIndex, 2L, app)
let context''' = writeRecord context'' (context''.NextIndex, 3L, app)
tryGet context' 9
fileStream.Close ()
let data = Array.zeroCreate<byte> 10000

let test2 () =
    let fs = new FileStream(@"c:\dump\without.dat", FileMode.OpenOrCreate,FileAccess.ReadWrite)
    fs.Seek(0L, SeekOrigin.End) |> ignore
    [0 .. 10000]
    |> List.iter (fun _ ->
        fs.Write(data, 0, data.Length)
        fs.Flush())
    fs.Close()

let test () =
    [0 .. 10000]
    |> List.iter (fun _ ->
        use fs = new FileStream(@"c:\dump\wevery.dat", FileMode.Append,FileAccess.Write)
        fs.Write(data, 0, data.Length)
        fs.Close())

#r "bin/Debug/FSharpx.Core.dll"
#r "bin/Debug/FsPickler.dll"
#load "Prelude.fs"

open System
open System.IO


Path.Combine("/", "/blah/behay/")
File.OpenWrite("").
Path.GetDirectoryName (@"c:\dump\blah\blah\blah.txt")

module Choice =
    let protect f p =
        try f p |> Choice1Of2
        with 
        | ex -> Choice2Of2 ex

module FileSystem =

    let combine x y =
        Path.Combine(x, y)

    let createDir p =
        Path.GetDirectoryName p
        |> Directory.CreateDirectory
        |> string

    let createFile p =
        File.Create p :> Stream

    let writeFile data p =
        File.WriteAllBytes(p, data)

                

    type FS =
        { Create : string -> Choice<Stream,exn>
          Write : string -> byte [] -> Choice<unit, exn> }
//          Open : string -> Choice<Stream,exn>
//          Delete : string -> Choice<unit,exn>
//          Exists : string -> bool }

    let init root =
        { Create = Choice.protect (fun p ->
                combine root p
                |> createDir
                |> createFile ) 
          Write = fun p ->
            Choice.protect (fun data ->
                combine root p
                |> createDir
                |> writeFile data )
                }
                
    
    
    type FSType =
        | Directory of Map<string, FSType> 
        | File of byte []
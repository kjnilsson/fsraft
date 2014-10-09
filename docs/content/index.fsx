(*** hide ***)
// This block of code is omitted in the generated HTML documentation. Use 
// it to define helpers that you do not want to show in the documentation.
#I "../../bin"
#r "../../bin/FsRaft.dll"

(**
F# Project Scaffold
===================

Documentation

<div class="row">
  <div class="span1"></div>
  <div class="span6">
    <div class="well well-small" id="nuget">
      The F# ProjectTemplate library can be <a href="https://nuget.org/packages/FSharp.ProjectTemplate">installed from NuGet</a>:
      <pre>PM> Install-Package FSharp.ProjectTemplate</pre>
    </div>
  </div>
  <div class="span1"></div>
</div>

Example
-------

This example demonstrates using a function defined in this sample library.

*)
open FsRaft
open System.IO
open System


//the apply function that applies a command to the state and returns the new state
let apply b state =
    (BitConverter.ToInt32(b, 0) + state)
    
let makeEndpoint (g: Guid) port =
    g, "127.0.0.1", port

let makeConfig endpoint =
    { Id = endpoint
      RpcFactory = None //this means it will use tcp 
      Register = id
      LogStream = new MemoryStream()
      TermStream = new MemoryStream() }

let makeNode endpoint config =
    let fsraft = new RaftAgent<int>(config, 0, apply)
    fsraft.Changes.Add (fun (_,_,state) -> printfn "%A Changed to: %A" endpoint state)
//    fsraft.LogEntry.Add (printfn "%A")
    fsraft.Error.Add (printfn "error %A")
    fsraft, endpoint, config

let make port =
    let endpoint = makeEndpoint (guid()) port
    let config = makeConfig endpoint
    makeNode endpoint config

let node1, ep1, config1 = make 3001
let node2, ep2, config2 = make 3002
let node3, ep3, config3 = make 3003

node1.Post (BitConverter.GetBytes 1)
//wait a few seconds here
node1.AddPeer ep2
node1.AddPeer ep3

//give it a few seconds


//add 1 to the intial state - run this a few times
//you should get output from all the nodes
node1.Post (BitConverter.GetBytes 1)

//shut down node1 (most likely the current leader node)
dispose node1
// give it a few seconds


//post another message to one of the surviving nodes
node3.Post (BitConverter.GetBytes 1)
node2.Post (BitConverter.GetBytes 1)

//revive node1
let node1' = makeNode ep1 (makeConfig ep1)


let n, _ , _ = node1'
n.Post (BitConverter.GetBytes 1)
(**
Some more info

Samples & documentation
-----------------------

The library comes with comprehensible documentation. 
It can include a tutorials automatically generated from `*.fsx` files in [the content folder][content]. 
The API reference is automatically generated from Markdown comments in the library implementation.

 * [Tutorial](tutorial.html) contains a further explanation of this sample library.

 * [API Reference](reference/index.html) contains automatically generated documentation for all types, modules
   and functions in the library. This includes additional brief samples on using most of the
   functions.
 
Contributing and copyright
--------------------------

The project is hosted on [GitHub][gh] where you can [report issues][issues], fork 
the project and submit pull requests. If you're adding a new public API, please also 
consider adding [samples][content] that can be turned into a documentation. You might
also want to read the [library design notes][readme] to understand how it works.

The library is available under Public Domain license, which allows modification and 
redistribution for both commercial and non-commercial purposes. For more information see the 
[License file][license] in the GitHub repository. 

  [content]: https://github.com/fsprojects/FSharp.ProjectScaffold/tree/master/docs/content
  [gh]: https://github.com/fsprojects/FSharp.ProjectScaffold
  [issues]: https://github.com/fsprojects/FSharp.ProjectScaffold/issues
  [readme]: https://github.com/fsprojects/FSharp.ProjectScaffold/blob/master/README.md
  [license]: https://github.com/fsprojects/FSharp.ProjectScaffold/blob/master/LICENSE.txt
*)

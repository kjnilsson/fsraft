namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("FsRaft.Program")>]
[<assembly: AssemblyProductAttribute("FsRaft")>]
[<assembly: AssemblyDescriptionAttribute("F# implementation of the Raft algorithm")>]
[<assembly: AssemblyVersionAttribute("1.0")>]
[<assembly: AssemblyFileVersionAttribute("1.0")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "1.0"

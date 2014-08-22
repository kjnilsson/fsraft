
type Machine =
    { Roles : string seq }

type Env =
    { Machines : Machine seq }


let envs = [{ Machines = [{ Roles = ["blah"; "bleh"] }; {Roles = ["yah"; "blah"] }] }]
envs
|> Seq.collect (fun x -> x.Machines) 
|> Seq.collect (fun x -> x.Roles) 
|> Seq.filter ((<>) "none")
|> Set.ofSeq
|> String.concat " "

[1;2]
|> Seq.filter ((<>) 1)

String.concat " "
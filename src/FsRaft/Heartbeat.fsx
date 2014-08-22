#r "bin/Debug/FSharpx.Core.dll"

#r "bin/Debug/FsPickler.dll"


#load "Prelude.fs"
#load "Persistence.fs"
#load "Model.fs"

#load "Heartbeat.fs"


type MaybeBuilder () =
    member __.Bind(x, f) = 
        match x with
        | Some x -> f x
        | None -> None
    member __.Delay(f) = f()
    member __.Return(x) = Some x

let maybe = MaybeBuilder()

let attempt() = Some 1

maybe {
    let! x = attempt()
    let! y = attempt ()
    return x + y}

System.String('a', 1)
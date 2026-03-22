(* Contains the signatures of core data operations *)
module type OPERATIONS = 
sig
    val map : ('a -> 'b) -> 'a Seq.t -> 'b Seq.t
    val filter : ('a -> bool) -> 'a Seq.t -> 'a Seq.t
    val fold_left : ('a -> 'b -> 'a) -> 'a -> 'b Seq.t -> 'a
    val fold_right: ('a -> 'b -> 'b) -> 'a Seq.t -> 'b -> 'b
    val mem : 'a -> 'a list Seq.t -> int -> bool
end

(* Module that implements the core data operations *)
module Operations : OPERATIONS =
struct
    
    let map f seq = 

        (* Helper function *)
        let rec aux seq = 
            match seq () with
            Seq.Nil -> Seq.Nil
            | Seq.Cons(h, t) -> Seq.Cons(f h, fun () -> (aux t)) in

        fun () -> aux seq
    
    let filter f seq = 

        (* Helper function *)
        let rec aux seq = 
            match seq () with
            Seq.Nil -> Seq.Nil
            | Seq.Cons(h, t) -> if ((f h) = true) then Seq.Cons(h, fun () -> (aux t))
                                else (aux t) in
                        
        fun () -> aux seq

    let rec fold_left f acc seq =

        match seq () with
        Seq.Nil -> acc
        | Seq.Cons(h, t) -> fold_left f (f acc h) t

    let rec fold_right f seq acc = 

        match seq () with 
        Seq.Nil -> acc
        | Seq.Cons(h, t) -> fold_right f t (f h acc)

    let rec mem el seq col_idx = 

        match seq () with
        Seq.Nil -> false
        | Seq.Cons(h, t) -> if ((List.nth h col_idx) = el) then true else mem el t col_idx
end
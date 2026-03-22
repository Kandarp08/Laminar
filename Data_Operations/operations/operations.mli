(* Contains the signatures of core data operations *)
module type OPERATIONS = 
sig

    (* Apply a given function on each element of the sequence *)
    val map : ('a -> 'b) -> 'a Seq.t -> 'b Seq.t

    (* Filter elements of a sequence which satisfy a particular function *)
    val filter : ('a -> bool) -> 'a Seq.t -> 'a Seq.t

    (* Update value of accumulator value by repeatedly applying a function *)
    val fold_left : ('a -> 'b -> 'a) -> 'a -> 'b Seq.t -> 'a
    
    (* Update value of accumulator value by repeatedly applying a function *)
    val fold_right: ('a -> 'b -> 'b) -> 'a Seq.t -> 'b -> 'b

    (* Find whether an element exists in a sequence or not *)
    val mem : 'a -> 'a list Seq.t -> int -> bool
end

module Operations : OPERATIONS
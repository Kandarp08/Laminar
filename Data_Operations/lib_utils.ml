open Datatypes
open Dataframe
open Data_object
open Row

open Operations
open Int_util
open Float_util

module type LIB_UTILS = 
sig
    val update_pos : 'a list -> 'a -> int -> int -> 'a list
end

module Lib_utils : LIB_UTILS = 
struct

    let rec update_pos l new_el pos curr_pos = 

        match l with 
        | [] -> []
        | h :: t -> 
            if pos = curr_pos then new_el :: t
            else h :: update_pos t new_el pos (curr_pos + 1)
end

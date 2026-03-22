open Datatypes
open Dataframe
open Data_object
open Row

open Int_util
open Float_util
open Operations
open Int_transformations
open Float_transformations
open Lib_utils

module type LIB =
sig

    (* Load CSV file *)
    val load_from_csv : string -> Dataframe.t
    
    (* Load JSON file*)
    val load_from_json : string -> Dataframe.t

    (* Display the dataframe *)
    val show_df : ?limit:int -> Dataframe.t -> unit
    
    (* Apply a given function to a column in the dataframe *)
    val map : (data_object -> data_object) -> string -> Dataframe.t -> Dataframe.t

    (* Filter rows of a dataframe whose columns satisfy a given function *)
    val filter : (data_object -> bool) -> string -> Dataframe.t -> Dataframe.t

    (* Find a given element in a particular column of the dataframe *)
    val mem : string -> data_object -> Dataframe.t -> bool

    (* Update value of accumulator by repeatedly applying a function *)    
    val fold_left : string -> (data_object -> data_object -> data_object) -> data_object -> Dataframe.t -> data_object
    
    (* Update value of accumulator by repeatedly applying a function *)
    val fold_right : string -> (data_object -> data_object -> data_object) -> data_object -> Dataframe.t -> data_object
    
    (* Sum of a given integer/float column of the dataframe *)
    val sum : string -> Dataframe.t -> data_object

    (* Length of a given column of the dataframe *)
    val len : string -> Dataframe.t -> data_object

    (* Add a new row to the dataframe *)
    val add_row : Row.t -> Dataframe.t -> Dataframe.t
    
    (* Delete the rows which satisfy a given condition *)
    val delete_row : (Row.t -> bool) -> Dataframe.t -> Dataframe.t

    (* Update the rows of the dataframe that satisfy a given condition *)
    val update_row : (Row.t -> bool) -> (Row.t -> Row.t) -> Dataframe.t -> Dataframe.t
end

module Lib : LIB
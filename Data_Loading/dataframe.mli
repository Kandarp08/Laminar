open Data_object
open Datatypes
open Row
open Utils

(** Dataframe type and operations *)

type dataframe = {
  filename: string;
  headers: string list;
  dtypes: datatype list;
  rows: unit -> Row.t Seq.t;
  ncols: int;
}

module Dataframe : sig
  type t = dataframe

  (** Get the index of a column by name *)
  val get_column_index : dataframe -> string -> int

  (** Get a column by name as a sequence of data objects *)
  val get_column : dataframe -> string -> data_object Seq.t

  (** Number of rows in the dataframe *)
  val no_of_rows : dataframe -> int

  (** Size of the dataframe: #rows x #cols *)
  val size : dataframe -> int

  (** Shape of the dataframe: (#rows, #cols) *)
  val shape : dataframe -> int * int

  (** Load a dataframe from a file *)
  val load_from_file : char -> string ->dataframe

  (** Load a dataframe from a CSV file *)
  val load_from_csv : string -> dataframe

  (** Load a dataframe from a JSON file *)
  val load_from_json : string -> dataframe

  (* Stores a dataframe as a CSV file *)
  val to_csv : dataframe -> string -> unit

  (* Stores a dataframe as a JSON file *)
  val to_json : dataframe -> string -> unit
end
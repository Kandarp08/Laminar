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
    val show_df : ?limit:int -> Dataframe.t -> unit
    val map : (data_object -> data_object) -> string -> Dataframe.t -> Dataframe.t
    val filter : (data_object -> bool) -> string -> Dataframe.t -> Dataframe.t
    val mem : string -> data_object -> Dataframe.t -> bool
    val fold_left : string -> (data_object -> data_object -> data_object) -> data_object -> Dataframe.t -> data_object
    val fold_right : string -> (data_object -> data_object -> data_object) -> data_object -> Dataframe.t -> data_object
    val sum : string -> Dataframe.t -> data_object
    val len : string -> Dataframe.t -> data_object
    val add_row : Row.t -> Dataframe.t -> Dataframe.t
    val delete_row : (Row.t -> bool) -> Dataframe.t -> Dataframe.t
    val update_row : (Row.t -> bool) -> (Row.t -> Row.t) -> Dataframe.t -> Dataframe.t
end

module Lib : LIB = 
struct

    let show_df ?(limit = 10) df =
        let display_rows = df.rows () |> Seq.take limit |> List.of_seq in
        
        let col_widths = Array.init df.ncols (fun i ->
            let header_width = String.length (List.nth df.headers i) in
            let max_data_width = 
                List.fold_left (fun acc_w row ->
                    let cell_val = DataObject.to_string (List.nth row i) in
                    max acc_w (String.length cell_val)
                ) 0 display_rows
            in
            max header_width max_data_width + 2 
        ) in

        let print_sep () =
            print_char '+';
            Array.iter (fun w -> print_string (String.make w '-') ; print_char '+') col_widths;
            print_newline ()
        in

        print_sep ();
        print_char '|';
        List.iteri (fun i h -> Printf.printf " %-*s |" (col_widths.(i) - 2) h) df.headers;
        print_newline ();
        print_sep ();

        (* Iterating over the list of rows *)
        List.iter (fun row ->
            print_char '|';
            List.iteri (fun i cell ->
                let s = DataObject.to_string cell in
                Printf.printf " %-*s |" (col_widths.(i) - 2) s
            ) row;
            print_newline ()
        ) display_rows;
        
        print_sep ();
        Printf.printf "Showing %d rows\n" (List.length display_rows)

    let map f col_name df = 
        let col_idx = Dataframe.get_column_index df col_name in
        let transform row = 
            let target = List.nth row col_idx in
            Lib_utils.update_pos row (f target) col_idx 0 in

        let new_rows_seq = Operations.map transform (df.rows ()) in 
        let new_rows_seq_temp = Operations.map transform (df.rows ()) in
        
        (* Use a temp call to peek at the type without consuming the main sequence *)
        let updated_dtypes = 
            match new_rows_seq_temp () with
            | Seq.Nil -> df.dtypes
            | Seq.Cons(first_row, _) -> 
                let new_val = List.nth first_row col_idx in
                let new_type = Data_object.DataObject.get_datatype new_val in
                Lib_utils.update_pos df.dtypes new_type col_idx 0
        in

        { df with dtypes = updated_dtypes; rows = fun () -> (fun () -> new_rows_seq ()) }

    let filter f col_name df = 
        let col_idx = Dataframe.get_column_index df col_name in
        let predicate row = f (List.nth row col_idx) in
        let filtered_rows = Operations.filter predicate (df.rows ()) in
        { df with rows = fun () -> (fun () -> filtered_rows ()) }

    let mem col_name el df = 
        let col_idx = Dataframe.get_column_index df col_name in
        let row_seq = df.rows () in
        let rec check s = match s () with
            | Seq.Nil -> false
            | Seq.Cons(row, t) -> if (List.nth row col_idx) = el then true else check t
        in check row_seq

    let fold_left col_name f init df = 
        let col_idx = Dataframe.get_column_index df col_name in
        let rec aux acc s = match s () with
            | Seq.Nil -> acc
            | Seq.Cons(row, t) -> aux (f acc (List.nth row col_idx)) t
        in aux init (df.rows ())

    let fold_right col_name f init df = 
        let col_idx = Dataframe.get_column_index df col_name in
        let rec aux s = match s () with
            | Seq.Nil -> init
            | Seq.Cons(row, t) -> f (List.nth row col_idx) (aux t)
        in aux (df.rows ())

    let apply_column_transformation transform_func col_name target_type df =
        let col_idx = Dataframe.get_column_index df col_name in
        let column = Dataframe.get_column df col_name in
        let transformed_col_list = transform_func column |> List.of_seq in
        
        let new_rows () = 
            let rec build_rows row_s col_l = 
                match row_s (), col_l with
                | Seq.Cons(r, rt), new_val :: vt -> 
                    Seq.Cons(Lib_utils.update_pos r new_val col_idx 0, fun () -> build_rows rt vt)
                | _ -> Seq.Nil
            in build_rows (df.rows ()) transformed_col_list
        in
        { df with 
            dtypes = Lib_utils.update_pos df.dtypes target_type col_idx 0;
            rows = fun () -> new_rows }

    let sum col_name df = 
        let col_idx = Dataframe.get_column_index df col_name in
        let dtype = List.nth df.dtypes col_idx in
        let col = Dataframe.get_column df col_name in
        match dtype with
        | INT -> INT_DATA (Int_util.sum col)
        | FLOAT -> FLOAT_DATA (Float_util.sum col)
        | _ -> failwith "Numeric type required"

    let len _ df = 
        let rec count s n = match s () with
            | Seq.Nil -> n
            | Seq.Cons(_, t) -> count t (n + 1)
        in INT_DATA (count (df.rows ()) 0)

    let add_row row df = 
        let rec validate r dt = match r, dt with
            | [], [] -> true
            | x::xt, y::yt -> if (DataObject.get_datatype x) = y then validate xt yt else false
            | _ -> false in
        if not (validate row df.dtypes) then failwith "Type mismatch";
        { df with rows = fun () -> (fun () -> Seq.Cons(row, df.rows ())) }

    let delete_row f df = 
        { df with rows = (fun () -> Seq.filter (fun r -> not (f r)) (df.rows ())) }

    let update_row f update df = 
        let rec validate r dt = match r, dt with
            | [], [] -> true
            | x::xt, y::yt -> if (DataObject.get_datatype x) = y then validate xt yt else false
            | _ -> false in
        let map_func r = 
            if f r then 
                let ur = update r in
                if validate ur df.dtypes then ur else failwith "Update type mismatch"
            else r in
        { df with rows = (fun () -> Seq.map map_func (df.rows ())) }

end
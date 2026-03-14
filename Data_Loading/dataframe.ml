open Datatypes
open Data_object
open Row
open Utils

type dataframe = {
  filename: string;
  headers: string list;
  dtypes: datatype list;
  rows: unit -> Row.t Seq.t; 
  ncols: int;
}

module Dataframe = struct
  type t = dataframe

  let get_column_index df colname = 
    let rec iter i =
      if i >= df.ncols then failwith "Column not found"
      else 
        if List.nth df.headers i = colname then i
        else iter (i+1) in
    iter 0

  (* Every call to rows() opens a new file handle *)
  let get_column df colname = 
    let col_index = get_column_index df colname in
    Seq.map (fun row -> List.nth row col_index) (df.rows ())

  let no_of_rows df = Seq.fold_left (fun acc _ -> acc + 1) 0 (df.rows ())

  let size df = df.ncols * no_of_rows df

  let shape df = (no_of_rows df, df.ncols)

  let load_from_file sep filepath = 
    let ic = open_in filepath in
    let headers, dtypes = 
      try
        let h_line = input_line ic in
        let h = List.map strip (String.split_on_char sep h_line) in
        let t_line = input_line ic in
        let t_strs = String.split_on_char sep t_line in
        if List.length t_strs <> List.length h then 
          failwith "Datatypes size mismatch";
        let t = List.map Datatype.string_to_datatype t_strs in
        (h, t)
      with e -> 
        close_in ic; raise e
    in
    close_in ic;
    let ncols = List.length headers in

    let row_generator () = 
      let file = open_in filepath in
      (* Skip the first two metadata lines *)
      ignore (input_line file);
      ignore (input_line file);
      
      let rec readlines () = 
        try
          let line = input_line file in
          let data = String.split_on_char sep line in
          let row = Row.row_from_string_list dtypes ncols data in
          Seq.Cons (row, readlines)
        with End_of_file -> 
          close_in file;
          Seq.Nil
      in
      readlines
    in
    { filename = filepath; headers; dtypes; rows = row_generator; ncols }

  let load_from_csv filepath = load_from_file ',' filepath

  let load_from_json filepath = 
    let file = open_in filepath in
    let rec read_all acc = 
      try read_all (acc ^ input_line file)
      with End_of_file -> close_in file; acc in
    let json_string = read_all "" in
    let json_obj = try JSON.parse json_string with _ -> failwith "JSON parse error" in
    
    let headers = JSON.get_value json_obj "columns" |> JSON_Value.array_to_list |> List.map JSON_Value.string_value_to_string in
    let dtypes = JSON.get_value json_obj "datatypes" |> JSON_Value.array_to_list |> List.map (fun v -> Datatype.string_to_datatype (JSON_Value.string_value_to_string v)) in
    let data_json = match JSON.get_value json_obj "data" with ARRAY a -> a | _ -> failwith "Data not array" in
    let ncols = List.length headers in

    {
      filename = filepath;
      headers;
      dtypes;
      rows = (fun () -> 
        List.to_seq data_json 
        |> Seq.map JSON_Value.array_to_list
        |> Seq.map (Row.row_from_json_value_list dtypes ncols)
      );
      ncols;
    }

  let to_csv df filepath = 
    let file = open_out filepath in
    Printf.fprintf file "%s\n" (output_str_list df.headers);
    Printf.fprintf file "%s\n" (output_str_list (List.map Datatype.datatype_to_string df.dtypes));
    Seq.iter (fun r -> 
      Printf.fprintf file "%s\n" (Row.row_to_csv df.dtypes r)
    ) (df.rows ());
    close_out file

  let to_json df filepath = 
    let file = open_out filepath in
    let cols = "[" ^ (output_str_list df.headers) ^ "]" in
    let dtypes = "[" ^ (output_str_list (List.map Datatype.datatype_to_string df.dtypes)) ^ "]" in
    
    (* Start the JSON object and the "data" array *)
    Printf.fprintf file "{\"columns\": %s, \"datatypes\": %s, \"data\": [" cols dtypes;
    
    let first = ref true in
    (* Stream the rows one by one *)
    Seq.iter (fun r ->
      if !first then 
        first := false 
      else 
        Printf.fprintf file ", "; (* Add comma before every row except the first *)
      Printf.fprintf file "%s" (Row.row_to_json_array r)
    ) (df.rows ());
    
    (* Close the "data" array and the root object *)
    Printf.fprintf file "]}\n";
    close_out file
end
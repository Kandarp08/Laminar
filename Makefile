DATA_LOADING_PATH = Data_Loading
DATA_OPERATIONS_PATH = Data_Operations
BENCHMARK_PATH = Benchmark

OCAMLC = ocamlc
OCAMLFLAGS = -I $(DATA_LOADING_PATH) \
             -I $(DATA_OPERATIONS_PATH) \
             -I $(DATA_OPERATIONS_PATH)/utils -I $(DATA_OPERATIONS_PATH)/operations \
             -I $(DATA_OPERATIONS_PATH)/transformations 

# File paths (Data Loading)
DATA_LOADING_UTILS = $(DATA_LOADING_PATH)/utils
DATATYPES = $(DATA_LOADING_PATH)/datatypes
DATAOBJECT = $(DATA_LOADING_PATH)/data_object
ROW = $(DATA_LOADING_PATH)/row
DATAFRAME = $(DATA_LOADING_PATH)/dataframe

# File paths (Data Operations)
UTILS = $(DATA_OPERATIONS_PATH)/utils/int_util $(DATA_OPERATIONS_PATH)/utils/float_util
OPERATIONS = $(DATA_OPERATIONS_PATH)/operations/operations
TRANSFORM = $(DATA_OPERATIONS_PATH)/transformations/int_transformations \
            $(DATA_OPERATIONS_PATH)/transformations/float_transformations
LIB_UTILS = $(DATA_OPERATIONS_PATH)/lib_utils
LIB = $(DATA_OPERATIONS_PATH)/lib
MAIN = main

# Lists of modules
MLI_MODULES = \
$(DATA_LOADING_UTILS:=.mli) $(DATATYPES:=.mli) $(DATAOBJECT:=.mli) $(ROW:=.mli) $(DATAFRAME:=.mli) \
$(OPERATIONS:=.mli) $(UTILS:=.mli) $(TRANSFORM:=.mli) $(LIB_UTILS:=.mli) $(LIB:=.mli) $(MAIN:=.mli)

ML_MODULES = \
$(DATA_LOADING_UTILS:=.ml) $(DATATYPES:=.ml) $(DATAOBJECT:=.ml) $(ROW:=.ml) $(DATAFRAME:=.ml) \
$(OPERATIONS:=.ml) $(UTILS:=.ml) $(TRANSFORM:=.ml) $(LIB_UTILS:=.ml) $(LIB:=.ml) $(MAIN:=.ml) $(BENCHMARK:=.ml)

CMIS = $(MLI_MODULES:.mli=.cmi)

CMOS = \
    $(DATA_LOADING_PATH)/utils.cmo \
    $(DATA_LOADING_PATH)/datatypes.cmo \
    $(DATA_LOADING_PATH)/data_object.cmo \
    $(DATA_LOADING_PATH)/row.cmo \
    $(DATA_LOADING_PATH)/dataframe.cmo \
    $(DATA_OPERATIONS_PATH)/operations/operations.cmo \
    $(DATA_OPERATIONS_PATH)/utils/int_util.cmo \
    $(DATA_OPERATIONS_PATH)/utils/float_util.cmo \
    $(DATA_OPERATIONS_PATH)/transformations/int_transformations.cmo \
    $(DATA_OPERATIONS_PATH)/transformations/float_transformations.cmo \
    $(DATA_OPERATIONS_PATH)/lib_utils.cmo \
    $(DATA_OPERATIONS_PATH)/lib.cmo

MAIN_OBJ = $(MAIN).cmo

TARGET = main.exe

.PHONY: all clean run run-benchmark

all: $(TARGET)

$(TARGET): $(CMIS) $(CMOS) $(MAIN_OBJ)
	$(OCAMLC) $(OCAMLFLAGS) -o $@ $(CMOS) $(MAIN_OBJ)

%.cmi: %.mli
	$(OCAMLC) $(OCAMLFLAGS) -c $<

%.cmo: %.ml
	$(OCAMLC) $(OCAMLFLAGS) -c $<

run: $(TARGET)
	./$(TARGET)

clean:
	rm -f *.cm[iox] *.o $(TARGET)
	find . -type f \( -name "*.cmo" -o -name "*.cmi" -o -name "*.o" -o -name "*.exe" -o -name "*.csv" -o -name "*test*.json" \) -exec rm -f {} +

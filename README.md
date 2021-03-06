# Universal_Data_Processor 
Note: This project is in Under-Development. A Use case with a dataset will be posted soon.

**This is an attempt to write an Universal Data Preprocessor using Python, Pandas, Numpy and Scipy. The data strorage part has been implemented using Apache Parquet which is based on HDFS.**

_Pre-requisite Packages:_

- Pandas
- Numpy
- Scipy
- Tensorflow
- Keras
- Apache Parquet
- Logging

_Let us consider a toy-dataset with the particular data-structure:_

**Student Database:**

- student_ID : int64 
- student_name : str
- student_DOB : datetime
- student_address: str
- student_telephone: int64
- student_height: float64
- student_weight: float64

**Files:**

**conv_to_parquet**

This function is a gateway to convert any CSV file into a parquet file following certain guidelines.

Inputs to be provided by the User:

- fname = CSV file to be processed and path as a string
- index_col = Column name to be indexed (Student ID in this case)
- cols = Columns to be kept in the output parquet file, in the form of a python list
- col_types = Column types of the cols to be kept in the parquet file, in the form of a python list
- out_fname = Ouput file name, path as a string
- nan_list = [] Columns that are supposed to have nan_list, in the form of a python list
- nan_list_types = [] Column types of the columns in the nan_list, in the form of a python list
- drop_nan = Default is False, if set to True, it drops all the rows with NAN values, if False, then it keeps all the rows with NAN values
- A CSV file containing the headers : In the form of a string
- Column names that the user needs to keep in the generated parquet file : In the form of a list
- column types of the columns that the user wants to keep: In the form of a list

[Note: Please make sure that the index of a column in the column names list and its datatype in the column types list should match]

**df_processes**

This .py file contains basic dataframe mainipulation techniques that are being used by data processors and readers.

- as_list --> Takes in data of type strings, tuples and returns a list item
- strip col --> Takes in a dataframe with required columns and strips the columns of any trailing spaces
- as_lower --> Takes in a dataframe with columns and converts the strings to lower-cases
- mapper --> takes in a column of a certain type and replaces a column values with its integer mapping. Returns modified dataframe and mapping file


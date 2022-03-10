# Mondrian k-Anonymity
Python implementation of Mondrian Multidimensional k-Anonymity algorithm
--------------------------------------------------------------------------------------------------------------------
Takes a table of raw data in .csv format and anonymises it to comply with the k-Anonymity privacy model using the algorithm described in:
LeFevre, K. et al. 2006. Mondrian Multidimensional k-Anonymity. Proceedings of the 22nd International Conference on Data Engineering (ICDE’06)

Input: .csv
Output: .csv

Usage: anonymise.py [input_filename] [QID_list (comma separated)] [k-value] [output_filename] [headers_used?(0=F,1=T)] [strict?(0=F,1=T)]

--------------------------------------------------------------------------------------------------------------------

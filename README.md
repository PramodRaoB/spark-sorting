# spark-sorting
Utilizing spark distributed framework for sorting SAM files, an intermediate step in genome sequencing pipeline

- Filters the input file to pre-process header lines
- Computes the offset in position corresponding to each RNAME with respect to their order in the header
- Filters the record lines and stores them in a paired RDD with the key being the processed position and the value being the line itself
- Different experiments mainly correspond to different kinds of partitioners being used
- A full detailed explanation can be found in the included PDF

### This directory displays the data structure branch of our project tree diagram.

*Notes*: 
    [1]: As noted in our report, we have a large amount of data. Therefore, we employed AWS with PostgreSQL for ingestion, storage, and SQL based wrangling to complement our python scripting.
    [2]: In each folder within this directory, we present small cross-section of the data in that phase, that is meant to be representative of the full dataset (i.e. we give a random sample of 1,000 tuples of a raw data file, 1,000 tuples of an interim data file, and 1,000 tuples of a processed data file).
    
     
    ├── interim        <- Intermediate data that has been transformed, `[i.e. wrangling steps]`.
    ├── processed      <- The final, canonical data sets for modeling, `[i.e. what we deploy in our models]`.
    └── raw            <- The original, immutable data dump, `[i.e. HMDA, other government agencies]`.
    

**Documentation:**  
  (1). Lists and Tuples in Python:  https://realpython.com/python-lists-tuples/  
  (2). Enumerating Lists in Python: https://www.afternerd.com/blog/python-enumerate/  
  (3). Encoding Categorical Data in Python:      https://medium.com/@rabinpoudyal1995/encoding-categorical-data-in-python-fab150d6e21b 
  (4). Use PostgreSQL to streamline Python Code: https://opensource.com/article/17/12/python-and-postgresql  
  (5). Read & Write from PostgreSQL with Security: https://creativedata.atlassian.net/wiki/spaces/SAP/pages/130318375/Python+-+Read+Write+tables+from+PostgreSQL+with+Security  
 
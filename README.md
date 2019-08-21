
<span style="color:navy">
============================================================================</span>  

**Team:** *Paddle Your Loan Canoe:*   
**Project:** *Predict This! Know Your Financing Approval Before the Lenders Do* <span style="color:navy">============================================================================</span>

**Cohort 15:** *Capstone Project for the Certificate of Data Science*  
**Georgetown University:** *School of Continuing Studies*  
*Summmer 2019*  



##### **Project Organization** -- The directory organization for our project follows the following structure:  
*------------*


    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    │
    ├── data_structure: presents our data, from raw to processed, including wrangling transformations.
    │   │
    │   ├── external       <- Data from third party sources, `[e.g. websites, social media, surveys, et. al.].`
    │   ├── interim        <- Intermediate data that has been transformed, `[i.e. wrangling steps]`.
    │   ├── processed      <- The final, canonical data sets for modeling, `[i.e. what we deploy in our models]`.
    │   └── raw            <- The original, immutable data dump, `[i.e. HMDA, other government agencies]`.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `01a_EDA__wrangling_intial_visuals__bbz.ipynb`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported (MAY NOT BE ABLE TO DO THIS)
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.testrun.org


--------

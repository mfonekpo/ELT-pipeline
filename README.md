# ELT Pipeline for Interesting Places in Nigeria


## Project Overview
This project aims to automate the extraction of information about interesting places in Nigeria from the hotels.ng website. Utilizing Python libraries such as BeautifulSoup and Prefect, the pipeline fetches, parses, and aggregates data on various tourist attractions, including their names, locations, categories, descriptions, and more. The initiative not only sharpens web scraping and data handling skills but also explores data analysis techniques to uncover insights within the gathered information.

## Key Features
Automated data extraction from hotels.ng using BeautifulSoup.
Robust data pipeline construction with Prefect for task orchestration.
Data aggregation and transformation to create a structured dataset.
Storing the processed data in a SQLite database and exporting to CSV format for further analysis.
Extensive data cleaning, exploration, and visualization to uncover insights about interesting places in Nigeria.
Technologies Used
Python: For scripting the entire data extraction, transformation, and loading process.
BeautifulSoup: To parse HTML content and extract relevant information.
Prefect: For orchestrating the tasks involved in the data pipeline.
SQLite: To store the structured data in a local database.
Pandas: For data manipulation and analysis.
Matplotlib & Seaborn: For data visualization.

## Project Structure:

```bash
interesting-places-ng/
│
├── datafiles/                # Folder to store exported CSV files
│
├── Analytics/ 
|
├── ETL_pipeline/
│   └── data_pipeline_mod.py    # Main script for data extraction and processing
│
└── README.md
|
└── requirements.txt
```

## Getting Started
To run this project locally, follow these steps:

- Clone the repository to your local machine.
- Ensure you have Python installed and set up a virtual environment.
- activate your virtual env: ```source venv_name/bin/activate```. venv_name should be the name of your virtual env
- start up the prefect server locally: ```prefect server start```
- Install the required dependencies: ```pip install -r requirements.txt```.
- Navigate to the ETL_pipeline/ directory and run python3 data_pipeline_mod.py.



## Future Improvements
Integration with Google Cloud Platform (GCP) for cloud storage solutions.
Expansion of the project scope to include more comprehensive data sources as they become available.

**Note**
- This project was initially created for practice purposes and to enhance web scraping, data analysis and data engineering skills. The information extracted is based on the data available on hotels.ng as of 2012, and might not reflect the most current data available.
- This project is still under developement.

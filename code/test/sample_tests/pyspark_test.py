# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("TestApp").getOrCreate()
# data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
# df = spark.createDataFrame(data, ["id", "name"])
# df.show()



# import json

# input_file = "/root/docker_dataset/big_data.json"
# output_folder = "/root/docker_dataset/json_splits/"
# chunk_size = 25000  # Number of records per split

# # Load JSON data safely
# with open(input_file, 'r') as infile:
#     data = json.load(infile)

# # Split and write smaller JSON files
# for i in range(0, len(data["values"]), chunk_size):
#     chunk = data["values"][i:i + chunk_size]
#     output_file = f"{output_folder}/big_data_split_{i//chunk_size}.json"
#     with open(output_file, 'w') as outfile:
#         json.dump({"values": chunk}, outfile, indent=2)
#     print(f"Written {output_file}")

# import json

# input_file = "/root/docker_dataset/json_splits/big_data_split_0.json"

# # Load JSON data safely
# with open(input_file, 'r') as infile:
#     data = json.load(infile)

# # Split and write smaller JSON files
# for i in data["values"][1:10]:
#      print (i.values())


# import requests
# from bs4 import BeautifulSoup
# import pandas as pd

# # URL of the webpage to scrape
# url = "https://h1bdata.info/index.php?em=hinduja+tech+inc&job=&city=&year=all+years"

# # Send HTTP request to the webpage
# response = requests.get(url)

# # Check if the request was successful
# if response.status_code == 200:
#     # Parse the content using BeautifulSoup
#     soup = BeautifulSoup(response.content, 'html.parser')
    
#     # Find all <a> tags that represent company links (inspect HTML for structure)
#     company_links = soup.find_all('a', href=True)
    
#     companies = []

#     # Loop through the <a> tags and extract company names
#     for link in company_links:

#          print(link.get_text().strip())
#         # The company name seems to be inside the <a> tag's text
#         company_name = link.get_text().strip()

#         # Some <a> tags might not be company names, check if the text is non-empty
#         if company_name and company_name != "More Info":
#             companies.append(company_name)
    
#     # Remove duplicates if there are any (company names should be unique)
#     companies = list(set(companies))
    
#     # Create a pandas DataFrame to structure the data
#     df = pd.DataFrame(companies, columns=["Company Name"])
    
#     # Save the DataFrame to a CSV file
#     df.to_csv("h1b_top_companies.csv", index=False)

#     # Or save it as an Excel file
#     # df.to_excel("h1b_top_companies.xlsx", index=False)
    
#     print("Data successfully scraped and saved to h1b_top_companies.csv")
# else:
#     print(f"Failed to retrieve the webpage: {response.status_code}")






import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time

# Function to scrape company data
def scrape_company_data(company_name):
    # Format the company name for the URL
    formatted_name = company_name.replace(" ", "+").lower()
    
    # Construct the URL
    url = f"https://h1bdata.info/index.php?em={formatted_name}&job=&city=&year=all+years"
    
    # Send HTTP request
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        # Parse the page content
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the table rows containing the data
        rows = soup.find_all("tr")[1:]  # Skip the header row

        data = []
        for row in rows:
            columns = row.find_all("td")
            if len(columns) == 6:  # Ensure it has the correct number of columns
                employer = columns[0].text.strip()
                job_title = columns[1].text.strip()
                base_salary = columns[2].text.strip()
                location = columns[3].text.strip()
                submit_date = columns[4].text.strip()
                start_date = columns[5].text.strip()

                data.append([employer, job_title, base_salary, location, submit_date, start_date])

        # Create DataFrame
        columns = ["Employer", "Job Title", "Base Salary", "Location", "Submit Date", "Start Date"]
        df = pd.DataFrame(data, columns=columns)
        
        # Save data to CSV
        company_file_name = f"{company_name.replace(' ', '_').lower()}_data.csv"
        df.to_csv(company_file_name, index=False)
        print(f"Data for {company_name} saved to {company_file_name}")
    else:
        print(f"Failed to retrieve data for {company_name}. HTTP Status Code: {response.status_code}")

# Read the list of company names from CSV
df_companies = pd.read_csv("h1b_top_companies.csv")

# Ensure output directory exists
if not os.path.exists("company_data"):
    os.makedirs("company_data")

# Change to the directory where data will be saved
os.chdir("company_data")

# Scrape data for each company
for company_name in df_companies["Company Name"]:
    scrape_company_data(company_name)
    time.sleep(2)  # Sleep to avoid potential rate-limiting

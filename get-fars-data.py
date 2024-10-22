import os
import requests

# Define the download URL template
base_url = "https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/{year}/National/FARS{year}NationalCSV.zip"


https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/2016/National/



# Define the output directory
output_dir = "/Users/marcrisney/Projects/jhu/mas/Fall2024/online-research-skills-spatial/data"
os.makedirs(output_dir, exist_ok=True)

# Loop through the years you want to download
for year in range(2012, 2022):
    download_url = base_url.format(year=year)
    output_path = os.path.join(output_dir, f"FARS{year}NationalCSV.zip")
    
    # Download the file
    print(f"Downloading FARS data for {year}...")
    response = requests.get(download_url)
    if response.status_code == 200:
        with open(output_path, "wb") as file:
            file.write(response.content)
        print(f"FARS data for {year} saved to {output_path}")
    else:
        print(f"Failed to download FARS data for {year}. Status code: {response.status_code}")

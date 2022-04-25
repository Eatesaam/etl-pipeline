import pandas as pd
import glob               
import xml.etree.ElementTree as ET
from datetime import datetime

def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process,lines=True)
    return dataframe

def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = dataframe.append({"name":name, "height":height, "weight":weight}, ignore_index=True)
    return dataframe


def extract():
    extracted_data = pd.DataFrame(columns=['name','height','weight']) # create an empty data frame to hold extracted data
    
    #process all csv files
    for csvfile in glob.glob("*.csv"):
        extracted_data = extracted_data.append(extract_from_csv(csvfile), ignore_index=True)
        
    #process all json files
    for jsonfile in glob.glob("*.json"):
        extracted_data = extracted_data.append(extract_from_json(jsonfile), ignore_index=True)
    
    #process all xml files
    for xmlfile in glob.glob("*.xml"):
        extracted_data = extracted_data.append(extract_from_xml(xmlfile), ignore_index=True)
        
    return extracted_data


def transform(data):
    #Convert height which is in inches to millimeter
    data['height'] = round(data.height * 0.0254,2)
        
    #Convert weight which is in pounds to kilograms
    data['weight'] = round(data.weight * 0.45359237,2)
    return data


def load(targetfile,data_to_load):
    data_to_load.to_csv(targetfile)  
    
    
def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')
        

def main():
    targetfile = "transformed_data.csv"
    # Log that you have started the ETL process
    log("ETL Job Started")

    # Log that you have started the Extract step
    log("Extract phase Started")
    # Call the Extract function
    extracted_data = extract()
    # Log that you have completed the Extract step
    log("Extract phase Ended")

    # Log that you have started the Transform step
    log("Transform phase Started")
    # Call the Transform function
    transformed_data = transform(extracted_data)
    # Log that you have completed the Transform step
    log("Transform phase Ended")

    # Log that you have started the Load step
    log("Load phase Started")
    # Call the Load function
    load(targetfile,transformed_data)
    # Log that you have completed the Load step
    log("Load phase Ended")

    # Log that you have completed the ETL process
    log("ETL Job Ended")
    

if __name__ == "__main__":
    main()
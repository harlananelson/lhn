import threading
import pandas as pd
from lhn.header import get_logger, F

logger = get_logger(__name__)

def clean_sheet_name(name):
    # Remove special characters and replace spaces with underscores
    cleaned_name = ''.join(e for e in name if e.isalnum() or e.isspace())
    cleaned_name = cleaned_name.replace(' ', '_')
    
    # Truncate to a reasonable length
    max_length = 31  # Excel's maximum sheet name length is 31 characters
    cleaned_name = cleaned_name[:max_length]
    
    return cleaned_name

def write_to_excel_thread(data, writer, sheet_name, cleaned_sheet_name):
    try:
        data.to_excel(writer, sheet_name=cleaned_sheet_name, index=False)
    except TimeoutError:
        print(f"Writing to {cleaned_sheet_name} took too long and has been terminated.")

def create_excel_spreadsheet_with_threads(data_sets, outFile, dataLoc, obsMax=10, SubjectsMin=5, maxTimePerIteration=60):
    writer = pd.ExcelWriter(dataLoc + outFile + '.xlsx', engine='xlsxwriter')
    
    for sheet_name, table_data in data_sets.items():
        cleaned_sheet_name = clean_sheet_name(sheet_name)
        print(cleaned_sheet_name)
        
        # Perform the data processing
        data = table_data.df.limit(obsMax).filter(F.col('Subjects') >= SubjectsMin).toPandas()
        
        # Create a thread to write data to Excel
        excel_thread = threading.Thread(
            target=write_to_excel_thread,
            args=(data, writer, sheet_name, cleaned_sheet_name)
        )
        excel_thread.start()
        excel_thread.join(timeout=maxTimePerIteration)
        
        if excel_thread.is_alive():
            print(f"Writing to {cleaned_sheet_name} took too long and has been terminated.")
            excel_thread.terminate()  # Terminate the thread
        
    writer.save()
    
def create_excel_spreadsheet(data_sets, outFile, dataLoc, obsMax = 10, SubjectsMin = 5):
    writer = pd.ExcelWriter(dataLoc + outFile + '.xlsx', engine='xlsxwriter')
    
    for sheet_name, table_data in data_sets.items():
        cleaned_sheet_name = clean_sheet_name(sheet_name)
        print(cleaned_sheet_name)
        data = table_data.df.limit(obsMax).filter(F.col('Subjects')>= SubjectsMin).toPandas()
        data.to_excel(writer, sheet_name=cleaned_sheet_name, index=False)
    
    writer.save()

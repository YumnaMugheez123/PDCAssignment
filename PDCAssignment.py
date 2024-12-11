import pandas as pd
from multiprocessing import Pool

# Load the data from CSV files
fees_data_path = 'C:/Users/Yumna Mugheez/Downloads/Fees_data.csv'
students_data_path = 'C:/Users/Yumna Mugheez/Downloads/Students_data.csv'

# Read the files with the correct delimiter (comma-separated)
fees_data = pd.read_csv(fees_data_path)
students_data = pd.read_csv(students_data_path)

# Check the first few rows and columns of the dataset to ensure the data is loaded
print("Fees Data Columns:", fees_data.columns)
print(fees_data.head())

# Convert 'date_of_fee_submission' to datetime format
fees_data['date_of_fee_submission'] = pd.to_datetime(fees_data['date_of_fee_submission'], errors='coerce', dayfirst=True)

# Clean and standardize name fields (just in case)
fees_data['FirstName'] = fees_data['first_name'].str.strip().str.title()
fees_data['LastName'] = fees_data['last_name'].str.strip().str.title()

# Function to calculate the most frequent submission date
def find_most_frequent_date(student_name):
    first_name, last_name = student_name
    # Filter the data for the student
    student_fee_data = fees_data[(fees_data['FirstName'] == first_name) & (fees_data['LastName'] == last_name)]
    
    if not student_fee_data.empty:
        # Find the most frequent fee submission date
        most_frequent_date = student_fee_data['date_of_fee_submission'].mode()
        if not most_frequent_date.empty:
            return f"{first_name} {last_name}'s Most Frequent Fee Submission Date: {most_frequent_date[0].strftime('%d-%m-%Y')}"
        else:
            return f"{first_name} {last_name} has no fee submissions recorded."
    else:
        return f"No data found for {first_name} {last_name}."



# Prepare the list of student names (this could be extended to handle multiple students)
students_to_process = [("John", "Doe")]

# Using multiprocessing to process the students in parallel
if __name__ == '__main__':
    with Pool(processes=4) as pool:  # Adjust the number of processes as needed
        results = pool.map(find_most_frequent_date, students_to_process)

    # Display the results
    for result in results:
        print(result)

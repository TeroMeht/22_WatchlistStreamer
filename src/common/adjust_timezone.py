from datetime import datetime, timedelta


# Function to adjust both 
def adjust_timezone_IB_data(date_value):
    # Ensure date_value is a string
    date_str = str(date_value)
    
    # Parse the date string into a datetime object
    original_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S%z")
    
    # Convert timezone from -4 to +7
    adjusted_date = (original_date + timedelta(hours=7)).strftime("%Y-%m-%d %H:%M")  # +7 - (-4) = 11

    # Return the adjusted date in the desired format

    return adjusted_date


# Function to adjust time only
def adjust_timezone_transactions(date_value):
    # Ensure date_value is a string
    date_str = str(date_value)
    
    # Parse the date string into a datetime object
    original_date = datetime.strptime(date_str, "%H:%M:%S")
    
    # Convert timezone from -4 to +7
    adjusted_date = (original_date + timedelta(hours=7)).strftime("%H:%M:%S")  # +7 - (-4) = 11
    
    # Return the adjusted date in the desired format
    return adjusted_date
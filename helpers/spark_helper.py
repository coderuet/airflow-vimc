from datetime import datetime
def generate_date_path_from_time(start_date : datetime, end_date : datetime):
    """Generate list of dates split by month between start_date and end_date.

    Args:
        start_date (datetime): The start date to process.
        end_date (datetime): The end date to process.
    """
    print("Processing dates...")
    # Logic to split dates by month and generate minio paths goes here
    list_paths = []
    current_date = start_date
    while current_date <= end_date:
        path_date = f"{current_date.year}/{current_date.month:02d}"
        list_paths.append(path_date)
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1, day=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1, day=1)
    return list_paths
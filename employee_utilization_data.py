import mysql.connector
import pandas as pd
import pygsheets


# Function connects mysql server and read the data from database
def connect():
    mydb = mysql.connector.connect(host="timesheetuat.cpjqq7sw0gwe.us-east-1.rds.amazonaws.com",
                                   user="da_user",
                                   password="password123",
                                   database="timesheet"
                                   )
    query = "SELECT CONCAT(UD.first_name, ' ', UD.last_name) AS " \
            "Employee_Name,(SELECT SUM(TIME_TO_SEC(T.task_hours))/3600 " \
            "FROM timesheet T WHERE T.user_id = UD.user_id AND T.task_date " \
            "BETWEEN '2023-01-01' AND '2023-02-28' AND T.project_id IN " \
            "(SELECT project_id FROM project_details WHERE billable = 1)) " \
            "AS Billable,(SELECT SUM(TIME_TO_SEC(T.task_hours))/3600 FROM " \
            "timesheet T WHERE T.user_id = UD.user_id AND T.task_date " \
            "BETWEEN '2023-01-01' AND '2023-02-28' AND T.project_id IN " \
            "(SELECT project_id FROM project_details WHERE utilization = 1 AND billable = 0)) " \
            "AS NON_Billable,(SELECT MIN(T.task_date) FROM timesheet T " \
            "WHERE T.user_id = UD.user_id AND T.task_date BETWEEN '2023-01-01' AND '2023-02-28') " \
            "AS Date_Range_From,(SELECT MAX(T.task_date) FROM timesheet T " \
            "WHERE T.user_id = UD.user_id AND T.task_date BETWEEN '2023-01-01' AND '2023-02-28') " \
            "AS Date_Range_To,(SELECT SUM(TIME_TO_SEC(T.calc_allocated_hours)) " \
            "FROM timesheet T WHERE T.user_id = UD.user_id AND T.status = 1 AND T.task_date " \
            "BETWEEN '2023-01-01' AND '2023-02-28') AS Calc_Allocated_Hours " \
            "FROM user_details UD WHERE UD.user_id IN (SELECT user_id FROM user_details) AND UD.active = 1"
    df = pd.read_sql(query, mydb)
    df['Date_Range_From'] = pd.to_datetime(df['Date_Range_From']).dt.strftime('%m/%d/%Y')
    df['Date_Range_To'] = pd.to_datetime(df['Date_Range_To']).dt.strftime('%m/%d/%Y')
    df = df.fillna('')
    return df


# Function to transfer the data from mysql data to Googlesheets
def transfer(df):
    path = 'C:\\Users\\Harish Reddy\\Downloads\\elevated-apex-375808-2e148ed27917.json'  # to replace the credential json
    gc = pygsheets.authorize(service_account_file=path)  # before authorizing share the sheet to the service account
    sh = gc.open('Timesheet Data')
    wk1 = sh[1]

    wk1.clear()  # remove previous data if any
    wk1.set_dataframe(df, (1, 1), extend=True)


if __name__ == "__main__":
    transfer(connect())

# code for transferring mysql data to GOOGLE SHEETS
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
    query = "SELECT project_details.project_name AS " \
            "Project_name,CONCAT(IFNULL(user_details.first_name, ''), " \
            "' ', IFNULL(user_details.last_name, '')) AS " \
            "Employee_name,IFNULL(project_details.budget, '') AS " \
            "budget,IFNULL(project_details.total_hours, '') AS " \
            "total_hours,IFNULL(project_details.sow_id, '') AS " \
            "sow_id,IFNULL(project_details.is_new_project, '') AS " \
            "is_new_project,CASE project_details.billable WHEN 1 " \
            "THEN 'Y' ELSE 'N' END AS billable,CASE project_details.utilization " \
            "WHEN 1 THEN 'Y' ELSE 'N' END AS utilization,SUM(timesheet.task_hours) " \
            "AS Hours_logged_for_billable_utilization_for_project," \
            "IFNULL(project_user_mapping.hourly_rate, '') AS " \
            "Hourly_rate_for_employee,SUM(timesheet.task_hours) * " \
            "IFNULL(project_user_mapping.hourly_rate, '') AS " \
            "Billing_rate_for_employee,SUM(SUM(timesheet.task_hours)) OVER " \
            "(PARTITION BY project_details.project_id) AS " \
            "Total_hours_logged_to_project,IFNULL(project_details.start_date, '') " \
            "AS start_date,IFNULL(project_details.end_date, '') AS " \
            "end_date,CASE project_details.active WHEN 1 THEN 'Y' ELSE 'N' END " \
            "AS active FROM project_details JOIN timesheet ON " \
            "project_details.project_id = timesheet.project_id JOIN user_details " \
            "ON timesheet.user_id = user_details.user_id JOIN project_user_mapping " \
            "ON timesheet.user_id = project_user_mapping.user_id AND " \
            "timesheet.project_id = project_user_mapping.project_id WHERE " \
            "project_details.billable = 1 AND project_details.active = 1 " \
            "GROUP BY project_details.project_id, user_details.user_id"

    df = pd.read_sql(query, mydb)
    return df


# Function to transfer the data from mysql data to Googlesheets
def transfer(df):
    path = 'C:\\Users\\Harish Reddy\\Downloads\\elevated-apex-375808-2e148ed27917.json'  # to replace the credential json
    gc = pygsheets.authorize(service_account_file=path)  # before authorizing share the sheet to the service account
    sh = gc.open('Timesheet Data')
    wk1 = sh[2]

    wk1.clear()  # remove previous data if any
    wk1.set_dataframe(df, (1, 1), extend=True)


if __name__ == "__main__":
    transfer(connect())

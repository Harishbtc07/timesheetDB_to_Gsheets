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

    # query="SELECT pd.project_name, COALESCE(pd.sow_id, 0) as sow_id, pd.total_hours, pd.budget, " \
    #       "concat(ud.first_name,' ',ud.last_name), COALESCE((SELECT (sum(TIME_TO_SEC(ts.task_hours)))/3600 from " \
    #       "timesheet ts where ts.project_id=pd.project_id), 0) as task_hours FROM project_details pd, user_details ud, " \
    #       "project_user_mapping pum WHERE pd.project_id=pum.project_id AND pum.user_id = ud.user_id AND pum.manager = 1 " \
    #       "ORDER BY pd.project_name;"

    query = "SELECT pd.Project_name, COALESCE(pd.Sow_id, '') AS " \
            "Sow_id, pd.Total_hours,CONCAT(ud.first_name, ' ', ud.last_name) AS " \
            "Manager, pd.Start_date, pd.End_date,COALESCE(pd.project_classification, '') " \
            "AS project_classification, pd.budget AS " \
            "Budget,CASE pd.billable WHEN 1 THEN 'Y' ELSE 'N' END AS " \
            "Billable,CASE pd.utilization WHEN 1 THEN 'Y' ELSE 'N' END AS " \
            "Utilization,COALESCE((SELECT (SUM(TIME_TO_SEC(ts.task_hours)))/3600 " \
            "FROM timesheet ts WHERE ts.project_id=pd.project_id), '') AS " \
            "utilizedHours FROM project_details pd, user_details ud, " \
            "project_user_mapping pum WHERE pd.project_id=pum.project_id AND " \
            "pum.user_id=ud.user_id AND pum.manager=1 AND pd.active=1 ORDER BY pd.Project_name"
    df = pd.read_sql(query, mydb)
    return df


# Function to transfer the data from mysql data to Googlesheets
def transfer(df):
    path = 'C:\\Users\\Harish Reddy\\Downloads\\elevated-apex-375808-2e148ed27917.json'  # to replace the credential json
    gc = pygsheets.authorize(service_account_file=path)  # before authorizing share the sheet to the service account
    sh = gc.open('Timesheet Data')
    wk1 = sh[0]

    wk1.clear()  # remove previous data if any
    wk1.set_dataframe(df, (1, 1), extend=True)


if __name__ == "__main__":
    transfer(connect())

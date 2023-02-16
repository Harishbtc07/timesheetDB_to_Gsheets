import mysql.connector#importing the mysql connector
import dao.Propertyfile.Prop as p

#creating the class for connection
class connection:
            try:
                conn = mysql.connector.connect(host=p.property.host,user=p.property.user,password=p.property.password,database=p.property.db)
                mycursor = conn.cursor()

            except mysql.connector.Error as e:
                print("Error reading data from MySQL table", e)

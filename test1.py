#!/usr/bin/python3
import requests
import csv
from time import gmtime, strftime
import mysql.connector

mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database=""
)
mycursor = mydb.cursor()
sql = "insert into data(date, time, imei, companyid, carplate, speed, longitude, longitudedirection, latitude, latitudedirection, powerdisconnection, sygnaldisconnection, Number) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
csvlist = []
today = strftime("%Y-%m-%d %H:%M:%S", gmtime())
with open('units.csv', 'rU') as csvfile:
    data = csv.reader(csvfile, delimiter=',')
    for i in data:
        mydict= {'Date':'', 'Time':' ', 'IMEI':i[2], 'Company Id':' ', 'Car Plate':i[1], 'Speed':'', 'Longitude':' ', 'Longitude Direction':' ', 'Latitude': '','Latitude Direction':'', 'Power Disconnection': '',
 'speed Sygnal Disconnection':'', 'Number':i[0]}
        csvlist.append(mydict)
url = 'http://198.38.92.224:7777/online?user=ombok&pass=abc123'
post_url = "http://41.206.37.78/speedlimiter/sg_data.php"
r = requests.get(url, stream = True, timeout=None)
print("All Went Well. Starting to streem data to database.\n")

for line in r.iter_lines():
        ext = line.decode('utf-8')
        text = ext.split(",")
        try:
            val = (text[0].split('|')[-1].split("=")[-1], text[11].split("=")[-1], text[12].split("=")[-1],text[17].split("=")[-1], text[-3].split("=")[-1], text[14].split("=")[-1], text[-8].split("=")[-1])
            for k in csvlist:
                if k["Number"] == val[0]:
                    if float(val[1]) > 0:
                        k["Longitude Direction"] = "East"
                    else:
                        k["Longitude Direction"] = "West"
                    if float(val[2]) > 0:
                        k["Latitude Direction"] = "North"
                    else:
                        k["Latitude Direction"] = "South"

                    if float(val[-1]) > 0:
                        k["Power Disconnection"] = "False"
                    else:
                        k["Power Disconnection"] = "True"
                    if float(val[-2]) > 0:
                        k["speed Sygnal Disconnection"] = "False"
                    else:
                        k["speed Sygnal Disconnection"] = "True"
                    k["Company Id"], k["Longitude"], k["Speed"], k["Latitude"], k["Date"], k["Time"] = "pinnacle systems", val[1], val[3], val[2], val[4].split(" ")[0], val[4].split(" ")[-1]
                    val = (k["Date"], k["Time"], k["IMEI"], k["Company Id"], k["Car Plate"], k["Speed"], k["Longitude"], k["Longitude Direction"], k["Latitude"], k["Latitude Direction"], k["Power Disconnection"], k["speed Sygnal Disconnection"], k["Number"])
                    print(k)
                    mycursor.execute(sql, val)
                    mydb.commit()
                    print(mycursor.rowcount, "record inserted.")
                    send = (str(k.values()).split("[")[1].split("]")[0])
                    send = send.replace("'", "")
                    send = send.replace(" ", '')
                    send = send[:-7]
                    r = requests.post(post_url, data=send)
                    print(send)
                    print(r.text)

                    with open("log.log", "w") as f:
                        f.write(str(send))
        except IndexError:
                print("\n")
                print("record discarded\n" )
                print(text)
                print("\n")

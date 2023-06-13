import requests
import json
import pandas as pd
import datetime, time
from io import BytesIO
import zipfile
import os
import mysql.connector
from sqlalchemy import create_engine
import glob

url = "https://api.ringba.com/v2/token"

payload='grant_type=password&username=mike%40carida.com&password=Rutgers-72'
headers = {
  'content-type': 'application/x-www-form-urlencoded; charset=UTF-8'
}

response = requests.request("POST", url, headers=headers, data=payload)

token = response.json()
token = token['access_token']

today = datetime.date.today()

# Calculate yesterday
yesterday = today - datetime.timedelta(days=1)

# If today is Monday (weekday() function returns 0 for Monday) then set yesterday to be last Friday
if today.weekday() == 0:
    yesterday = today - datetime.timedelta(days=3)

# Convert yeserday variable to string
yesterday = str(yesterday)

# This section is not required anymore as you're not using 'day' and 'today' variables anywhere
# day = datetime.date.today()
# today = time.strftime("%Y-%m-%d")

# Calculate start and end time for the given day
start = yesterday + 'T' + '00:00:00Z'
end = yesterday + 'T' + '23:59:59Z'

url = "https://api.ringba.com/v2/RA0909aa9eddae40c4bb48f7253e6e8742/calllogs/export/csv"

payload = json.dumps({
  "reportEnd": end,
  "reportStart": start,
  "orderByColumns": [
      {
          "column": "callDt",
          "direction": "desc"
      }
  ],
  "valueColumns": [
    {
      "column": "callDt"
    },
    {
      "column": "tag:DialedNumber:Name"
    },
    {
      "column": "campaignName"
    },
    {
      "column": "publisherName"
    },
    {
      "column": "targetName"
    },
    {
      "column": "targetNumber"
    },
    {
      "column": "buyer"
    },
    {
      "column": "inboundPhoneNumber"
    },
    {
      "column": "number"
    },
    {
      "column": "endCallSource"
    },
    {
      "column": "hasConnected"
    },
    {
      "column": "callLengthInSeconds"
    },
    {
      "column": "connectedCallLengthInSeconds"
    },
    {
      "column": "hasPayout"
    },
    {
      "column": "isDuplicate"
    },
    {
      "column": "hasPreviouslyConnected"
    },
    {
      "column": "hasConverted"
    },
    {
      "column": "payoutAmount"
    },
    {
      "column": "tag:InboundNumber:State"
    },
  ]
})
headers = {
  'Authorization': 'Bearer' + ' ' + token,
  'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)

csvid = response.json()['id']

url = "https://api.ringba.com/v2/RA0909aa9eddae40c4bb48f7253e6e8742/calllogs/export/" + csvid
payload={}
headers = {
  'Authorization': 'Bearer' + ' ' + token
  }
response = requests.request("GET", url, headers=headers, data=payload)
#time.sleep(420)
r = response.json()

'''poll the API until the file is ready'''
while True:
    response = requests.request("GET", url, headers=headers, data=payload)
    if response.json()['status'] == 'Ready':
        break
    else:
        time.sleep(5)
        continue
# response = requests.request("GET", url, headers=headers, data=payload)
r = response.json()['url']

print('Downloading started')

filename = url.split('/')[-1]

req = requests.get(r)
print('Downloading finished')

'''extract the file to the current directory'''
with zipfile.ZipFile(BytesIO(req.content)) as zip_ref:
    zip_ref.extractall()

    df = pd.concat(map(pd.read_csv, glob.glob('ringba-call-log*.csv')))

    # Step: Change data type of ['Campaign', 'Publisher', 'Target', 'Buyer', 'End Call Source', 'Connected', 'Paid Out', 'Is Duplicate', 'Converted'] to String/Text
for column_name in df.select_dtypes(['object']).columns:
    df[column_name] = df[column_name].astype('string')

# Step: Change data type of ['Caller ID', 'Number'] to String/Text
for column_name in ['Caller ID', 'Number']:
    df[column_name] = df[column_name].astype('string')

# Step: Change data type of ['Call Length', 'Connected Call Length', 'Payout'] to Integer
for column_name in df.select_dtypes('float').columns:
    df[column_name] = df[column_name].astype('Int64')

# Step: Drop rows where Payout is missing
df = df.loc[~(df['Payout'].isna())]

df['Call Date'] = pd.to_datetime(df['Call Date'], unit='ms')

df = df.drop(columns=['Converted', 'Campaign'])


# Step: Keep rows where Publisher is one of: PX Media
PX = df.loc[df['Publisher'].isin(['PolicyBind'])]

sql_str = 'mariadb+pymysql://mike@data.caridamedsupp.com:Rutgers-72@45.79.131.179/Customers'
          
# Create the connection
cnx = create_engine(sql_str)

PX.to_sql('PolicyBind Ringba Call Log', con=cnx, if_exists='append', index=False)

'''delete the file that begins with ringba-call-log'''
for file in glob.glob('ringba-call-log*.csv'):
    os.remove(file)

auth_token = "vu332i5d8jx6ppqyhmkq76sxs2e2w6fh"
start_time = start
end_time = end
limit = 500  # You can use any value between 1 and 500 for the limit parameter
offset = 0  # Set the offset to 0 to start pagination from the beginning
order = "asc"  # Set the order to "asc" to retrieve the data in ascending order
call_type =  '' # Set the call_type to "inbound" to retrieve only inbound call data
called_count = '' # Set the called_count to 0 to retrieve only calls that were not answered
include_recordings = 0  # Set the include_recordings to 0 to exclude recordings from the response
list_id = 3116

def convoso_api_to_dataframe(auth_token, start_time, end_time, limit, offset, order, call_type, called_count, include_recordings):
    url = f"https://api.convoso.com/v1/log/retrieve?auth_token={auth_token}&status=&list_id={list_id}&start_time={start_time}&end_time={end_time}&limit={limit}&offset={offset}&order={order}&call_type={call_type}&called_count={called_count}&include_recordings={include_recordings}"
    response = requests.get(url)
    data = response.json()['data']['results']
    df = pd.json_normalize(data)
    return df


# Create an empty dataframe to store the concatenated data
df_all = pd.DataFrame()

# Iterate over a range of values for the offset parameter
for offset in range(0, 40000, 500):
    # Call the function and store the returned dataframe in a variable
    df = convoso_api_to_dataframe(auth_token, start_time, end_time, limit, offset, order, call_type, called_count, include_recordings)
    
    # Concatenate the dataframe with the df_all dataframe
    df_all = pd.concat([df_all, df], ignore_index=True)


import pandas as pd; import numpy as np
# Step: Keep rows where list_id is one of: 2907
df_all = df_all.loc[df_all['list_id'].isin(['3116'])]

import pandas as pd; import numpy as np
# Step: Select columns
df_all = df_all[['phone_number', 'status_name', 'call_date']]

# Step: Change data type of call_date to Datetime
df_all['call_date'] = pd.to_datetime(df_all['call_date'], infer_datetime_format=True)

# Step: Change data type of status_name to String/Text
df_all['status_name'] = df_all['status_name'].astype('string')

# Step: Change data type of phone_number to String/Text
df_all['phone_number'] = df_all['phone_number'].astype('string')

import pandas as pd; import numpy as np
# Step: Create new column 'phone' from formula ''1' + phone_number'
df_all['phone'] = '1' + df_all['phone_number']

# Step: Drop columns
df_all = df_all.drop(columns=['phone_number'])

import pandas as pd; import numpy as np
# Step: Select columns
PX = PX[['Call Date', 'Target Number', 'Caller ID']]

# Step: Change data type of Target Number to String/Text
PX['Target Number'] = PX['Target Number'].astype('string')

# Step: Rename column
PX = PX.rename(columns={'Caller ID': 'phone'})

merged_df = pd.merge(PX, df_all, on=['phone'], how='left')

# Merged df_all and PX into df3
temp_df = PX.drop_duplicates(subset=['phone']) # Remove duplicates so lookup merge only returns first match
PX_tmp = temp_df.drop(['Target Number', 'Call Date'], axis=1)
df3 = df_all.merge(PX_tmp, left_on=['phone'], right_on=['phone'], how='left', suffixes=['_df_all', '_PX'])


import os
import smtplib
import mimetypes
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.mime.base import MIMEBase
import pandas as pd

def send_email_with_attachment():
    # Define the CSV file name and DataFrame object
    csv_file = 'merged_data.csv'
    df3 = pd.DataFrame()  # Replace with your actual DataFrame

    # Save DataFrame to CSV
    df3.to_csv(csv_file, index=False)

    # Define the email sender and recipient
    email_from = "marketing@caridamedsupp.com"
    email_to = ["reporting@policybind.com", "brian@policybind.com", "conor@policybind.com", "msavenko@gmail.com"]
    #email_to = ["mike@carida.com"]

    # Convert the email_to list to a comma-separated string
    email_to_str = ",".join(email_to)

    # Create the email message
    msg = MIMEMultipart()
    msg["From"] = email_from
    msg["To"] = email_to_str
    msg["Subject"] = "Carida Disposition Report"
    msg.preamble = "This is a report of the merged data."

    # Attach the CSV file to the email
    ctype, encoding = mimetypes.guess_type(csv_file)
    if ctype is None or encoding is not None:
        ctype = "application/octet-stream"
    maintype, subtype = ctype.split("/", 1)

    with open(csv_file, "rb") as fp:
        attachment = MIMEBase(maintype, subtype)
        attachment.set_payload(fp.read())
        encoders.encode_base64(attachment)
        attachment.add_header("Content-Disposition", "attachment", filename=csv_file)

    msg.attach(attachment)

    # Send the email
    try:
        smtp = smtplib.SMTP("mail.caridamedsupp.com")
        smtp.login("marketing@caridamedsupp.com", "Carida123")
        smtp.sendmail(email_from, email_to, msg.as_string())
        smtp.quit()
        print("Email sent successfully!")
    except Exception as e:
        print(f"An error occurred while sending the email: {str(e)}")

# Call the function to send the email
send_email_with_attachment()


from flask import Flask, render_template, request, redirect, url_for
import os
from os.path import join, dirname, realpath
import sys
import pandas as pd
from flask_wtf import FlaskForm
from wtforms import StringField
from wtforms.validators import DataRequired
from sqlalchemy import create_engine
from datetime import datetime

now = datetime.now()
app = Flask(__name__)

    # enable debugging mode
app.config["DEBUG"] = True

    # Upload folder(Given my Local Path)
UPLOAD_FOLDER = 'E:/Projects/Venzo/Flask_Upload/static/files/'
app.config['UPLOAD_FOLDER'] =  UPLOAD_FOLDER


Username= ''
Password = ''
Hostname = ''

    # Database initializations
sqlEngine_decision = create_engine('mysql+pymysql://' + str(Username) + ':' + 
                                                    str(Password) + '@' + 
                                                    str(Hostname) + ':3306/') 
dbConnection_decision = sqlEngine_decision.connect()


    # Root URL
@app.route('/api/file-import/')
def index():
     #  Uploaded HTML template '\templates\index.html'
    return render_template('index.html')


# Get the uploaded files
@app.route("/api/file-import/", methods=['POST'])
def uploadFiles():
      
        # get the uploaded file
    uploaded_file = request.files['file']
            
            #Checking File exist or not
    if uploaded_file.filename != '':
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], uploaded_file.filename)
        Name = request.form.get("username")
        Schema = request.form.get("schema")
           
                #Validating the name and 
        if Name == 'ashish' and Schema == 'public':
            uploaded_file.save(file_path) 
            parseCSV(file_path, Schema)
            
    return redirect(url_for('index'))

def parseCSV(filePath, Schema):
      # CVS Column Names
    col_names = ['first_name','last_name','address', 'street', 'state' , 'zip']
      # Use Pandas to parse the CSV file
    csvData = pd.read_csv(filePath,names=col_names, header=None)
      # Loop through the Rows
      
    for i,row in csvData.iterrows():
        sql = """INSERT INTO """+str(Schema)+"""."""+"""public.master_study_list_2022_01_21_17_09_11 (first_name, last_name, address, street, state, zip, Created_Date) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        value = (row['first_name'],row['last_name'],row['address'],row['street'],row['state'],str(row['zip']), str(now.strftime("%d_%m_%Y_%H_%M_%S")))
        Closed_DOFD_Result = dbConnection.execute(Closed_DOFD_Query).fetchone()
        dbConnection_decision.execute(sql, value, if_exists='append')   

if (__name__ == "__main__"):
    app.run(port = 5200)

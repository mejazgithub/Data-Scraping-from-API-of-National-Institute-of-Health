


import requests
import pandas as pd
import json
import datetime
import mysql.connector as mysql
import argparse


#argument parsing
def pars_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("year",help="Input the year you want to retrieve data for")
    parser.add_argument("offsetcounter",type=int,help="Value from which record you want to start fetching",default=0,nargs='?')
    parser.add_argument("output",help="Define output type csv or mysql")
    args = parser.parse_args()

    return args


# post request for getting data
def postrequest(url,data):

    
    result=requests.post(url, json=data)


    return result


#######################################################################################################################################
################################################################# DataBase Insertion functions ##################################################
#######################################################################################################################################

# application data insertion
def appldatainsertion(db,cursor,resultjsoni):

    appldataquery = "INSERT INTO appldata (default_appl_id, subproject_id , fiscal_year, org_name, org_city,org_state, dept_type, project_num, project_serial_num, org_country, activity_code, award_amount, is_active , project_start_date, project_end_date, foa, project_abstract , project_title, phr_text) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    appldataval =(resultjsoni['appl_id'],resultjsoni['subproject_id'],resultjsoni['fiscal_year'], resultjsoni['org_name'],resultjsoni['org_city'],resultjsoni['org_state'],resultjsoni['dept_type'],resultjsoni['project_num'],resultjsoni['project_serial_num'],resultjsoni['org_country'],resultjsoni['activity_code'],resultjsoni['activity_code'],resultjsoni['award_amount'],resultjsoni['is_active'],resultjsoni['project_start_date'],resultjsoni['project_end_date'],resultjsoni['abstract_text'],resultjsoni['project_title'],resultjsoni['phr_text'])
    cursor.execute(appldataquery, appldataval)
    
    db.commit()    



#agency data insertion 
def agencydatainsertion(db,cursor,resultjsoni):
    
    agencydataquery = "INSERT INTO agencyicfundings (appl_data_id,fy,code,name ,abbreviation ,total_cost) VALUES (%s,%s,%s, %s, %s, %s)"
    
    agencylist = resultjsoni['agency_ic_fundings']


    cursor.execute("SELECT appl_data_id FROM appldata WHERE default_appl_id = {0}".format(str(resultjsoni['appl_id'])))
    appldataid= cursor.fetchall()


    if agencylist is not None:
        if len(agencylist)!=0:

            for j in range(len(agencylist)):

                agencydataval =(appldataid[0][0],agencylist[j]['fy'], agencylist[j]['code'],agencylist[j]['name'],agencylist[j]['abbreviation'],agencylist[j]['total_cost'])
                cursor.execute(agencydataquery, agencydataval)

            
    else:
        agencydataval =(appldataid[0][0],"None","None","None","None","None")
        cursor.execute(agencydataquery, agencydataval)
    
    
    db.commit()
    



# program officers data
def podatainsertion(db,cursor,resultjsoni):
    
    program_officersquery = "INSERT INTO programofficers (appl_data_id, first_name, middle_name, last_name, full_name, email) VALUES (%s,%s, %s, %s, %s, %s)"
    
    polist = resultjsoni['program_officers']


    cursor.execute("SELECT appl_data_id FROM appldata WHERE default_appl_id = {0}".format(str(resultjsoni['appl_id'])))
    appldataid= cursor.fetchall()

    if polist is not None:

        if len(polist)!=0:

            for j in range(len(polist)):
                
                program_officersval =(appldataid[0][0],polist[j]['first_name'], polist[j]['middle_name'],polist[j]['last_name'],polist[j]['full_name'],polist[j]['email'])        
                cursor.execute(program_officersquery, program_officersval)
    
    else:
        
        program_officersval =(appldataid[0][0],"None","None","None","None","None")
        cursor.execute(program_officersquery, program_officersval)
    
    db.commit()
    


#principle investigator data insertion
def pidatainsertion(db,cursor,resultjsoni):
    
    principalinvestigatorsquery = "INSERT INTO principalinvestigators (appl_data_id, profile_id , first_name, middle_name, last_name, is_contact_pi, full_name, title, email) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s)"
    pilist = resultjsoni['principal_investigators']
 


    cursor.execute("SELECT appl_data_id FROM appldata WHERE default_appl_id = {0}".format(str(resultjsoni['appl_id'])))
    appldataid= cursor.fetchall()

    if  pilist is not None:

        if len(pilist)!=0:

            
            for j in range(len(pilist)):

                principalinvestigatorsval =(appldataid[0][0],pilist[j]['profile_id'], pilist[j]['first_name'],pilist[j]['middle_name'],pilist[j]['last_name'],pilist[j]['is_contact_pi'],pilist[j]['full_name'],pilist[j]['title'],pilist[j]['email'])
                cursor.execute(principalinvestigatorsquery, principalinvestigatorsval)
    else:
        principalinvestigatorsval =(appldataid[0][0],"None", "None", "None", "None", "None", "None", "None", "None")
        cursor.execute(principalinvestigatorsquery, principalinvestigatorsval)
        

        
    db.commit()
    




#create database connection and create database if not created
def dbcreation():

    db = mysql.connect(host = "localhost",
    	user = "YOURUSERNAME",
    	passwd = "YOURUSERPASSOWRD"
    	#port = 8888, #for Mamp users
    	)

    #select cursor
    cursor = db.cursor()


    #create database if not existe
    cursor.execute("CREATE DATABASE IF NOT EXISTS NIH_DataBase")

    # select the created database
    cursor.execute("USE NIH_DataBase ")


    cursor.execute("SET CHARACTER SET utf8mb4") 

    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")    
    ## create common application data table
    cursor.execute("CREATE TABLE IF NOT EXISTS appldata (appl_data_id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, default_appl_id VARCHAR(255) NOT NULL,subproject_id VARCHAR(255), fiscal_year VARCHAR(255), org_name VARCHAR(255), org_city VARCHAR(255),org_state VARCHAR(255), dept_type VARCHAR(255), project_num VARCHAR(255), project_serial_num VARCHAR(255), org_country VARCHAR(255), activity_code VARCHAR(255), award_amount VARCHAR(255), is_active VARCHAR(255), project_start_date VARCHAR(255), project_end_date VARCHAR(255), foa VARCHAR(255), project_abstract TEXT, project_title VARCHAR(255), phr_text TEXT)")

    # create agencies table
    cursor.execute("CREATE TABLE IF NOT EXISTS agencyicfundings (id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, appl_data_id INT(11) NOT NULL, fy VARCHAR(255),code VARCHAR(255),name VARCHAR(255),abbreviation VARCHAR(255),total_cost VARCHAR(255), FOREIGN KEY (appl_data_id) REFERENCES appldata(appl_data_id))")

    # create program officers table
    cursor.execute("CREATE TABLE IF NOT EXISTS programofficers (id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, appl_data_id INT(11) NOT NULL, first_name VARCHAR(255),middle_name VARCHAR(255),last_name VARCHAR(255),full_name VARCHAR(255),email VARCHAR(255),FOREIGN KEY (appl_data_id) REFERENCES appldata(appl_data_id))")

    # create principle investigators table
    cursor.execute("CREATE TABLE IF NOT EXISTS principalinvestigators (id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, appl_data_id INT(11) NOT NULL, profile_id VARCHAR(255),first_name VARCHAR(255), middle_name VARCHAR(255),last_name VARCHAR(255), is_contact_pi VARCHAR(255),full_name VARCHAR(255),title VARCHAR(255),email VARCHAR(255),FOREIGN KEY (appl_data_id) REFERENCES appldata(appl_data_id) )")

    return db,cursor

#save data in mysql database
def savedataindb(resultjson):
    #create db connection and tables
    db,cursor= dbcreation()

    # run loop on 50 fetched records
    for i in range(len(resultjson['results'])):

        appldatainsertion(db,cursor,resultjson['results'][i])
        
        agencydatainsertion(db,cursor,resultjson['results'][i])

        podatainsertion(db,cursor,resultjson['results'][i])
        
        pidatainsertion(db,cursor,resultjson['results'][i])

#######################################################################################################################################
#################################################################DataBase Functions end here ##################################################
#######################################################################################################################################




#######################################################################################################################################
#################################################################CSV Functions ########################################################
#######################################################################################################################################


# processing 50 records fetched per request
def processrecordsforcsv (resultjson,recordfetched):

    for rf in range(recordfetched):


        ''' Building data structure for csv '''

        seperator=','        # convert nested dict of program officers to list
        if resultjson['results'][rf]['program_officers']:

            pofullname= []
            for po in range(len(resultjson['results'][rf]['program_officers'])):
                pofullname.append(resultjson['results'][rf]['program_officers'][po]['full_name'])


            #converting list to a string, values separated with , 
            listtostr= seperator.join(pofullname) 
            resultjson['results'][rf]['program_officers'] = listtostr

        # adding none if the officers list is empty 
        else:
            resultjson['results'][rf]['program_officers'] = None



        # convert nested dict of agency_ic_fundings to a list 
        if resultjson['results'][rf]['agency_ic_fundings']:

            agency_ic_fundings=[]
            for agency in range(len(resultjson['results'][rf]['agency_ic_fundings'])):
                agency_ic_fundings.append(resultjson['results'][int(rf)]['agency_ic_fundings'][int(agency)]['name'])


            #converting list to a string, values separated with ,
            listtostring= seperator.join(agency_ic_fundings)
            resultjson['results'][rf]['agency_ic_fundings']= listtostring

        # adding none if the agency list is empty   
        else:
            resultjson['results'][rf]['agency_ic_fundings']= None




        # convert nested dict of pi to a list
        if resultjson['results'][rf]['principal_investigators']:

            projectinvestigatorsid=[]
            projectinvestigatorsname=[]

            for pi in range(len(resultjson['results'][rf]['principal_investigators'])):

                projectinvestigatorsid.append(str(resultjson['results'][rf]['principal_investigators'][pi]['profile_id']))
                projectinvestigatorsname.append(resultjson['results'][rf]['principal_investigators'][pi]['full_name'])

                resultjson['results'][rf]['ProjectInvestigatorsName']= projectinvestigatorsname 
                resultjson['results'][rf]['ProjectInvestigatorsId']= projectinvestigatorsid 

            listttostring= seperator.join(projectinvestigatorsname)
            resultjson['results'][rf]['ProjectInvestigatorsName']= listttostring

            listtttostring= seperator.join(projectinvestigatorsid)
            resultjson['results'][rf]['ProjectInvestigatorsId']= listtttostring
        else:

            resultjson['results'][rf]['ProjectInvestigatorsName']= None
            resultjson['results'][rf]['ProjectInvestigatorsId']= None


        # converting result json to a data frame
        df = pd.DataFrame.from_dict(resultjson['results'])

    return df


#########################################################################################################################################
###########################################################$ CSV Functions End $###########################################################
#########################################################################################################################################




# Main Funcition
def main():

    args= pars_arguments()


    # api url
    url = 'https://api.reporter.nih.gov/v1/projects/Search'

    # output format parameter
    params = { 'format': 'json'}

    #data to search for
    data=    {
            "criteria":
            {
                "year": [args.year]
            },
        "include_fields": [
           "appl_id","subproject_id","fiscal_year","org_name","org_city",
           "org_state","dept_type", "project_num","project_serial_num","org_country","activity_code","award_amount","is_active",
             "principal_investigators","program_officers","agency_ic_fundings",
             "project_start_date","project_end_date","foa","abstract_text","project_title", "phr_text"],
            "offset":args.offsetcounter,
             "limit":50,
             "sort_field":"project_start_date",
              "sort_order":"desc"
       }
   

    print('Fetching Data for Year ',args.year)


    if args.output=='mysql':

        totalrecords=0
        #run loop for 10,000 records
        for totalrecords in range(195):
            

            #post request for first time
            resultjson = postrequest(url,data)

            savedataindb(resultjson)

            print(data['offset']," records have been saved to database")

            if data['offset']<9996:

                #change offset value
                data['offset']+=51
                #update totalrecods value
                totalrecords+=1

                #if in an iregular case offset value increases from 9996
                if data['offset']>9996:
                    data['offset']==9996

            # to change the order of records so we can fetch last         
            elif data['offset']==9996 and data['sort_order']=='desc':

                #change sorting order of records to fetch more records
                data['sort_order']=='asce'
                #reset totalrecords value
                totalrecords=0
                #reset offsetcounter value
                data['offset']=0

            # to break the loop after fetchinga all the records 
            elif data['offset']==9996 and data['sort_order']=='asce':
                break

    elif args.output=='csv':

        totalrecords=0
        dataframes=[]

        #run loop for 10,000 records
        for totalrecords in range(195):


            #post request to get data
            result= postrequest(url,data)    

            if result.status_code==200 and result.text!='':

                resultjson=result.json()
                #process fifty fetched records
                df = processrecordsforcsv(resultjson, len(resultjson['results']))

                #append fifty processed records in a list
                dataframes.append(df)

                print(data['offset']," records have been saved")

            if data['offset']<9996:

                #change offset value
                data['offset']+=51

                #update totalrecords value
                totalrecords+=1

            # to change the order of records so we can fetch last         
            elif data['offset']==9996 or data['offset']+51>9996:
                if data['sort_order']=='desc':

                    print("Changing sorting order of nih data to asce")

                    data['sort_order']=='asce'
                    totalrecords=0
                    data['offset']=0

                elif data['sort_order']=='asce':

                    #merging list of data frames into one data frame
                    df2= pd.concat(dataframes)
                    #renaming header column of data frame
                    del df2['principal_investigators']

                    maindataframe = df2.set_axis(['ApplicationId', 'SubprojectId', 'FiscalYear','OrganisationName',
                        'OrganisationCity','OrganisationState','DepartmentType','ProjectNumber','ProjectSerialNumber','OrganisationCountry',
                        'ActivityCode','AwardAmount','IsActive','ProgramOfficers','AgencyICFundig','ProjectStartDate','ProjectEndDate',
                        'FundingOpportunityAnnouncements','AbstractText','ProjectTitle','PHRText',
                        'ProjectInvestigatorsName','ProjectInvestigatorsId'], axis=1, inplace=False)

                    print("Saving data to csv")
                    #saving data to csv
                    maindataframe.to_csv('NIH_Data_'+args.year+'_'+str(data['offset'])+'.csv')

                    break


      
  
  
if __name__ == '__main__':
    main()

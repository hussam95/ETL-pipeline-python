import pandas as pd
import pyodbc
import numpy as np

#===================================== Pandas/Extract =============================================================
# Import CSV
data = pd.read_excel(r"C:\\Users\\Hussam\Desktop\\RT Fisher\\rt_fisher_data.xlsx", sheet_name="Q1 Grades")   

#===================================== Pandas/Transform ============================================================

# Preprocess data
df = pd.DataFrame(data)
imp_cols=df.columns[1:].to_list()
df = df[imp_cols]

# Pick first trimester data (dim-students) or pick all (facts-performance/schools) or pick filtered (exception-joins t2) 
# joined_t2_ids = [
#                     id 
#                     for id in (df[df["Trimester"]==2]["Student ID"].unique().tolist())
#                     if 
#                     id not in (df[df["Trimester"]==1]["Student ID"].unique().tolist())
#                 ]

df = df[df["Quarter"]==2] # choosing 2nd quarter data because it has more unique student_ids and first last names

#df = df[df['Student ID'].isin(joined_t2_ids)]


old_cols = ["Student ID","Reported Race","Special ED","Gender","Ind Study",
            "Socio-Ec Disadv.",504,"McKV/Homeless","Last Name",
            "First Name"]

new_cols = ["STUDENT_ID", "REPORTED_RACE","STUDENT_IS_SPECIAL","GENDER","IND_STUDY","SOCIO_ECO_DIS","FIVE_HUNDRED_FOUR",
            "MCKVHOMELESS", "LAST_NAME","FIRST_NAME"]

# Replace messy col names with sql-compliant col names
mapper={}

for index,col in enumerate(old_cols):
  mapper[col]=new_cols[index]

df=df.rename(columns=mapper)

df=df[new_cols]

print(df.info())


# Deal with missing values
for col in new_cols:
    if df[col].dtype == object:
        df[col]=df[col].fillna("NA")
        df[col]=df[col].astype(str)
    else:
        df[col]=df[col].fillna(0)
        

#print(df.info())

#===================================== SqlServer/Load =============================================================

# Let's Connect to RTFISHER Database in our SqlServer
try:
    conn = pyodbc.connect('Driver={SQL Server};'
                    'Server=LAPTOP-57F3LA9L;'
                    'Database=RTFISHER_HIGH;'
                    'Trusted_Connection=yes;')
    
    print("Connected successfully to SqlServer database!")
    
except Exception as e:
    print("Unable to connect to SqlServer Database")

# Cursor object to execute SQL queries
cursor = conn.cursor()

# Create Dimension Table
cursor.execute('''
		CREATE TABLE Students (
			STUDENT_ID int primary key,
			"FIRST_NAME" char(200),
            "LAST_NAME" char(200),
            GENDER char(50),
			REPORTED_RACE char(50),
            STUDENT_IS_SPECIAL char(50),
            FIVE_HUNDRED_FOUR char(50),
            SOCIO_ECO_DIS char(50),
            MCKVHOMELESS char(50),
            IND_STUDY char(50)
            
            
			)
               ''')
conn.commit()

#Insert DataFrame to Table
c=0
for row in df.itertuples():
    try:
        cursor.execute('''
                    INSERT INTO Students (STUDENT_ID,"FIRST_NAME","LAST_NAME",GENDER,REPORTED_RACE,STUDENT_IS_SPECIAL,
                    FIVE_HUNDRED_FOUR,SOCIO_ECO_DIS,MCKVHOMELESS,IND_STUDY)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    ''' ,
                    row.STUDENT_ID,
                    row.FIRST_NAME,
                    row.LAST_NAME,
                    row.GENDER,
                    row.REPORTED_RACE,
                    row.STUDENT_IS_SPECIAL,
                    row.FIVE_HUNDRED_FOUR,
                    row.SOCIO_ECO_DIS,
                    row.MCKVHOMELESS,
                    row.IND_STUDY

                    
        )

        c+=1

    
    
    except Exception as e:
        print(e)             

print(f"{c} records added successfully to students")   

                
conn.commit()

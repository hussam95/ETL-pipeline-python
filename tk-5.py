import pandas as pd
import pyodbc
import numpy as np
#===================================== Pandas/Extract =============================================================

# Import CSV
data = pd.read_excel(r"C:\\Users\\Hussam\Desktop\\RT Fisher\\rt_fisher_data.xlsx", sheet_name="TK-5")   

#===================================== Pandas/Transform ============================================================
# Preprocess data
df = pd.DataFrame(data)
imp_cols=df.columns[2:].to_list()
df = df[imp_cols]

# Pick first trimester data only
df=df[df["Trimester"]==1]

old_cols = ["Student ID","Gender","Reported Race","Student Is Special Ed?",504,
            "SED from CALPADS","McKV/ Homeless","Suspensions through 10/25","Attendance through 10/15",
            "Report Card: English TCRWP level","Report Card: Spanish TCRWP level","Report Card: Op & Alg Thinking",
            "Report Card: Num & Ops in Base Ten","Report Card: Measurement and Data","Report Card: Geometry",
            "Report Card: Math Fluency","STAR Reading District BC Name","STAR Math District BC Name",
            "Dibels Composite Level","First Name","Last Name","Year","Trimester", "Vision Scholars"]

new_cols = ["STUDENT_ID","GENDER","REPORTED_RACE","STUDENT_IS_SPECIAL","FIVE_HUNDRED_FOUR","SED_FROM_CALPADS",
            "MCKVHOMELESS","SUSPENSIONS","ATTENDANCE","ENGLISH", "SPANISH","OP_ALG_THINKING","NUM_OPS",
            "MEASUREMENT_AND_DATA","GEOMETRY","MATH_FLUENCY","STAR_READING","STAR_MATH","DIBELS",
            "FIRST_NAME","LAST_NAME","YEAR","TRIMESTER", "VISION_SCHOLARS"]

# Replace messy col names with sql-compliant col names
mapper={}

for index,col in enumerate(old_cols):
  mapper[col]=new_cols[index]

df=df.rename(columns=mapper)

df=df[new_cols]

#print(df.info())


# Deal with missing values
for col in new_cols:
    if df[col].dtype == object:
        df[col]=df[col].fillna("NA")
        df[col]=df[col].astype(str)
    else:
        df[col]=df[col].fillna(0)
        

#print(df.info())

#===================================== SqlServer/Load =============================================================
# Connect to SQL Server
try:
    conn = pyodbc.connect('Driver={SQL Server};'
                    'Server=LAPTOP-57F3LA9L;'
                    'Database=RTFISHER;'
                    'Trusted_Connection=yes;')
    
except Exception as e:
    print(e)

cursor = conn.cursor()

# Create Table
cursor.execute('''
		CREATE TABLE ElementaryFull (
			STUDENT_ID int primary key,
			GENDER char(50),
			REPORTED_RACE char(50),
            STUDENT_IS_SPECIAL char(50),
            FIVE_HUNDRED_FOUR char(50),
            SED_FROM_CALPADS char(50),
            MCKVHOMELESS char(50),
            SUSPENSIONS decimal,
            ATTENDANCE decimal(5,4),
            "ENGLISH" char(200),
             "SPANISH" decimal,
             "OP_ALG_THINKING" decimal,
             "NUM_OPS" decimal,
            "MEASUREMENT_AND_DATA" char(200),
            "GEOMETRY" decimal,
            "MATH_FLUENCY" decimal,
            "STAR_READING" char(200),
            "STAR_MATH" char(200),
            "DIBELS" char(200),
            "FIRST_NAME" char(200),
            "LAST_NAME" char(200),
            "YEAR" char(200),
            "TRIMESTER" int,
            "VISION_SCHOLARS" char(200)

            
			)
               ''')
conn.commit()


# Insert DataFrame to Table
for row in df.itertuples():
    try:
        cursor.execute('''
                    INSERT INTO ElementaryFull (STUDENT_ID,GENDER,REPORTED_RACE,STUDENT_IS_SPECIAL,
                    FIVE_HUNDRED_FOUR,SED_FROM_CALPADS,MCKVHOMELESS,SUSPENSIONS,ATTENDANCE,"ENGLISH",
                     "SPANISH","OP_ALG_THINKING","NUM_OPS","MEASUREMENT_AND_DATA",
                     "GEOMETRY","MATH_FLUENCY","STAR_READING","STAR_MATH","DIBELS",
                     "FIRST_NAME","LAST_NAME","YEAR","TRIMESTER","VISION_SCHOLARS")
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ''' ,
                    row.STUDENT_ID,
                    row.GENDER,
                    row.REPORTED_RACE,
                    row.STUDENT_IS_SPECIAL,
                    row.FIVE_HUNDRED_FOUR,
                    row.SED_FROM_CALPADS,
                    row.MCKVHOMELESS,
                    row.SUSPENSIONS,
                    row.ATTENDANCE,
                    row.ENGLISH,
                    row.SPANISH,
                    row.OP_ALG_THINKING,
                    row.NUM_OPS,
                    row.MEASUREMENT_AND_DATA,
                    row.GEOMETRY,
                    row.MATH_FLUENCY,
                    row.STAR_READING,
                    row.STAR_MATH,
                    row.DIBELS,
                    row.FIRST_NAME,
                    row.LAST_NAME,
                    row.YEAR,
                    row.TRIMESTER,
                    row.VISION_SCHOLARS
                    
        )
    except Exception as e:
        print(e)                

                
conn.commit()



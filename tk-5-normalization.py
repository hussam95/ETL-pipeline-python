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

# Pick first trimester data (dim-students) or pick all (facts-performance/schools) or pick filtered (exception-joins t2) 
joined_t2_ids = [
                    id 
                    for id in (df[df["Trimester"]==2]["Student ID"].unique().tolist())
                    if 
                    id not in (df[df["Trimester"]==1]["Student ID"].unique().tolist())
                ]



df = df[df['Student ID'].isin(joined_t2_ids)]


old_cols = ["Student ID","Gender","Reported Race","Student Is Special Ed?",504,
            "SED from CALPADS","McKV/ Homeless","Suspensions through 10/25","Attendance through 10/15",
            "Report Card: English TCRWP level","Report Card: Spanish TCRWP level","Report Card: Op & Alg Thinking",
            "Report Card: Num & Ops in Base Ten","Report Card: Measurement and Data","Report Card: Geometry",
            "Report Card: Math Fluency","STAR Reading District BC Name","STAR Math District BC Name",
            "Dibels Composite Level","First Name","Last Name","Year","Trimester", "Vision Scholars", "School",
            "Grade Level"]

new_cols = ["STUDENT_ID","GENDER","REPORTED_RACE","STUDENT_IS_SPECIAL","FIVE_HUNDRED_FOUR","SED_FROM_CALPADS",
            "MCKVHOMELESS","SUSPENSIONS","ATTENDANCE","ENGLISH", "SPANISH","OP_ALG_THINKING","NUM_OPS",
            "MEASUREMENT_AND_DATA","GEOMETRY","MATH_FLUENCY","STAR_READING","STAR_MATH","DIBELS",
            "FIRST_NAME","LAST_NAME","YEAR","TRIMESTER", "VISION_SCHOLARS", "SCHOOL","GRADE_LEVEL"]

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

# Let's Connect to RTFISHER Database in our SqlServer
try:
    conn = pyodbc.connect('Driver={SQL Server};'
                    'Server=LAPTOP-57F3LA9L;'
                    'Database=RTFISHER_ELEMENTARY;'
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
            SED_FROM_CALPADS char(50),
            MCKVHOMELESS char(50)
            
            
			)
               ''')
conn.commit()

#Insert DataFrame to Table
c=0
for row in df.itertuples():
    try:
        cursor.execute('''
                    INSERT INTO Students (STUDENT_ID,"FIRST_NAME","LAST_NAME",GENDER,REPORTED_RACE,STUDENT_IS_SPECIAL,
                    FIVE_HUNDRED_FOUR,SED_FROM_CALPADS,MCKVHOMELESS)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    ''' ,
                    row.STUDENT_ID,
                    row.FIRST_NAME,
                    row.LAST_NAME,
                    row.GENDER,
                    row.REPORTED_RACE,
                    row.STUDENT_IS_SPECIAL,
                    row.FIVE_HUNDRED_FOUR,
                    row.SED_FROM_CALPADS,
                    row.MCKVHOMELESS

                    
        )

        c+=1

    
    
    except Exception as e:
        print(e)             

print(f"{c} records added successfully to students")   

                
conn.commit()

#Create Facts Table
cursor.execute('''
		CREATE TABLE Performance (
			STUDENT_ID int foreign key references Students(STUDENT_ID),
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
            "YEAR" char(200),
            "TRIMESTER" int,
            "VISION_SCHOLARS" char(200)

            
			)
               ''')
conn.commit()


# # Insert DataFrame to Facts Table
c=0
for row in df.itertuples():
    try:
        cursor.execute('''
                    INSERT INTO Performance (STUDENT_ID, SUSPENSIONS, ATTENDANCE, "ENGLISH",
                     "SPANISH", "OP_ALG_THINKING", "NUM_OPS", "MEASUREMENT_AND_DATA",
                     "GEOMETRY", "MATH_FLUENCY", "STAR_READING", "STAR_MATH", "DIBELS",
                     "YEAR", "TRIMESTER", "VISION_SCHOLARS")
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ''' ,
                    row.STUDENT_ID,
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
                    row.YEAR,
                    row.TRIMESTER,
                    row.VISION_SCHOLARS
                    
        )

        c+=1
    except Exception as e:
        print(e) 


print(f"{c} records added successfully to performance table")


conn.commit()


#Schools, grade level incorporation 


try:
    conn = pyodbc.connect('Driver={SQL Server};'
                    'Server=LAPTOP-57F3LA9L;'
                    'Database=RTFISHER_ELEMENTARY;'
                    'Trusted_Connection=yes;')
    
    print("Connected successfully to SqlServer database!")
    
except Exception as e:
    print("Unable to connect to SqlServer Database")

# Cursor object to execute SQL queries
cursor = conn.cursor()

# Create another facts Table 'Schools'
cursor.execute('''
		CREATE TABLE Schools (
			STUDENT_ID int foreign key references Students(STUDENT_ID),
            GRADE_LEVEL char(200),
            SCHOOL char (300)
            
			)
               ''')
conn.commit()

for row in df.itertuples():
    try:
        cursor.execute('''
                    INSERT INTO Schools (STUDENT_ID, GRADE_LEVEL, SCHOOL)
                    VALUES (?,?,?)
                    ''' ,
                    row.STUDENT_ID,
                    row.GRADE_LEVEL,
                    row.SCHOOL
        )

      
    except Exception as e:
        print(e) 

conn.commit()
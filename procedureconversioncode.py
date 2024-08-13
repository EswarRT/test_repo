import cx_Oracle

# Define your Oracle database connection details
dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='orcl.lan')
conn = cx_Oracle.connect(user='rbs', password='rbs', dsn=dsn_tns)

# Create a cursor object
cur = conn.cursor()

# Clear the male_female_ratio table before inserting new data
clear_table_sql = "DELETE FROM male_female_ratio"
cur.execute(clear_table_sql)

# Calculate male-to-female ratios and insert into the ratio table
insert_ratio_sql = """
INSERT INTO male_female_ratio (Pclass, Survived, Male_Count, Female_Count, Male_Female_Ratio)
SELECT 
    Pclass,
    Survived,
    SUM(CASE WHEN Sex = 'Male' THEN 1 ELSE 0 END) AS Male_Count,
    SUM(CASE WHEN Sex = 'female' THEN 1 ELSE 0 END) AS Female_Count,
    ROUND(SUM(CASE WHEN Sex = 'Male' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN Sex = 'female' THEN 1 ELSE 0 END), 0), 2) AS Male_Female_Ratio
FROM 
    TITANIC_DATA
GROUP BY 
    Pclass, 
    Survived
"""
cur.execute(insert_ratio_sql)

# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()

print("Male-to-female ratio calculation and insertion completed.")

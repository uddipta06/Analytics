import requests
import json
import mysql.connector as myconn

# Database Connection
my_db = myconn.connect(
    host="localhost",
    user="root", 
    password="Password12345?",
    database="Project_Guvi"
)

# Create Tables
with my_db.cursor() as db_cursor:
    db_cursor.execute("""
        CREATE TABLE IF NOT EXISTS complexes (
            complex_id VARCHAR(50) PRIMARY KEY,
            complex_name VARCHAR(100) NOT NULL
        )
    """)

    db_cursor.execute("""
        CREATE TABLE IF NOT EXISTS venues (
            venue_id VARCHAR(50) PRIMARY KEY,
            venue_name VARCHAR(100) NOT NULL,
            city_name VARCHAR(100) NOT NULL,
            country_name VARCHAR(100) NOT NULL,
            country_code CHAR(3) NOT NULL,
            timezone VARCHAR(100) NOT NULL,
            complex_id VARCHAR(50),
            FOREIGN KEY (complex_id) REFERENCES complexes(complex_id)
        )
    """)

    my_db.commit()

# Fetch Data from API
def fetch_data():
    url = "https://api.sportradar.com/tennis/trial/v3/en/complexes.json?api_key=AIpdUAQrBr4Gl7iorsj7fzMuvsRKKpFaUMcMJZLK"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()

    print(f"Error {response.status_code}: {response.text}")
    return None

# Insert Complexes Data
def insert_complexes():
    data = fetch_data()  # Fetch data from API
    if data and "complexes" in data:
        complexes = data["complexes"]

        with my_db.cursor() as db_cursor:
            for comp in complexes:
                db_cursor.execute("""
                    INSERT IGNORE INTO complexes (complex_id, complex_name)
                    VALUES (%s, %s)
                """, (comp.get("id"), comp.get("name")))

            my_db.commit()
        
        print("Complexes Data Inserted Successfully!")
    else:
        print("No complex data available or API issue.")

# Insert Venues Data
def insert_venues():
    data = fetch_data()  # Fetch data from API
    if data and "complexes" in data:
        complexes = data["complexes"]

        with my_db.cursor() as db_cursor:
            for comp in complexes:
                complex_id = comp.get("id")

                for venue in comp.get("venues", []):
                    db_cursor.execute("""
                        INSERT IGNORE INTO venues 
                        (venue_id, venue_name, city_name, country_name, country_code, timezone, complex_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        venue.get("id"), venue.get("name"),
                        venue.get("city", "Unknown"), venue.get("country", "Unknown"),
                        venue.get("country_code", "N/A"), venue.get("timezone", "N/A"),
                        complex_id
                    ))
            
            my_db.commit()

        print("Venues Data Inserted Successfully!")
    else:
        print("No venue data available or API issue.")


if __name__ == "__main__":
    insert_complexes()
    insert_venues()

my_db.close()


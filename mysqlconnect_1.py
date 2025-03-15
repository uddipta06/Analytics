import requests
import json
import mysql.connector as myconn

# Database Connection
my_db = myconn.connect(host="localhost", username="root", password="Password12345?", database="Project_Guvi")
db_cursor = my_db.cursor()

# Create Categories Table
db_cursor.execute("""
    CREATE TABLE IF NOT EXISTS Categories (
        category_id VARCHAR(50) PRIMARY KEY,
        category_name VARCHAR(50) NOT NULL
    )
""")

# Create Competitions Table
db_cursor.execute("""
    CREATE TABLE IF NOT EXISTS Competitions (
        competition_id VARCHAR(50) PRIMARY KEY,
        competition_name VARCHAR(100) NOT NULL,
        parent_id VARCHAR(50) NULL,
        type VARCHAR(20) NOT NULL,
        gender VARCHAR(10) NOT NULL,
        category_id VARCHAR(50),
        FOREIGN KEY (category_id) REFERENCES Categories(category_id)
    )
""")

my_db.commit()

# Fetch Data from API
def fetch_data(endpoint):
    url = "https://api.sportradar.com/tennis/trial/v3/en/competitions.json?api_key=AIpdUAQrBr4Gl7iorsj7fzMuvsRKKpFaUMcMJZLK"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()

    print(f"Error {response.status_code}: {response.text}")
    return None

# Insert Categories Data
def insert_categories():
    data = fetch_data("competitions")
    if data and "competitions" in data:
        competitions = data["competitions"]

        categories_inserted = set()

        for comp in competitions:
            category_id = comp["category"]["id"]
            category_name = comp["category"]["name"]

            if category_id not in categories_inserted:
                db_cursor.execute("""
                    INSERT IGNORE INTO Categories (category_id, category_name)
                    VALUES (%s, %s)
                """, (category_id, category_name))
                categories_inserted.add(category_id)

        my_db.commit()
        print("Categories Data Inserted Successfully!")
    else:
        print("No category data available or endpoint issue.")

# Insert Competitions Data
def insert_competitions():
    data = fetch_data("competitions")
    if data and "competitions" in data:
        competitions = data["competitions"]

        for comp in competitions:
            category_id = comp["category"]["id"]

            db_cursor.execute("""
                INSERT IGNORE INTO Competitions 
                (competition_id, competition_name, parent_id, type, gender, category_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (comp["id"], comp["name"], comp.get("parent_id"), comp["type"], comp["gender"], category_id))

        my_db.commit()
        print("Competitions Data Inserted Successfully!")
    else:
        print("No competition data available or endpoint issue.")

if __name__ == "__main__":
    insert_categories()
    insert_competitions()

db_cursor.close()
my_db.close()

import mysql.connector
import requests

# API Configuration
API_KEY = "AIpdUAQrBr4Gl7iorsj7fzMuvsRKKpFaUMcMJZLK"
BASE_URL = "https://api.sportradar.com/tennis/trial/v3/en/double_competitors_rankings.json"

# MySQL Configuration
MYSQL_USER = "root"
MYSQL_PASSWORD = "Password12345?"
MYSQL_HOST = "localhost"
MYSQL_DB = "Project_Guvi"

# Connect to MySQL
def get_db_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )

# Create Tables
def create_tables():
    connection = get_db_connection()
    cursor = connection.cursor()

    # Create Competitors Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS competitors (
            competitor_id VARCHAR(50) PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            country VARCHAR(100) NOT NULL,
            country_code CHAR(3) NOT NULL,
            abbreviation VARCHAR(10) NOT NULL
        )
    """)

    # Create Rankings Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS competitor_rankings (
            rank_id INT AUTO_INCREMENT PRIMARY KEY,
            ranking_position INT NOT NULL,
            movement INT NOT NULL,
            points INT NOT NULL,
            competitions_played INT NOT NULL,
            competitor_id VARCHAR(50),
            FOREIGN KEY (competitor_id) REFERENCES competitors(competitor_id) ON DELETE CASCADE
        )
    """)

    connection.commit()
    cursor.close()
    connection.close()

# Fetch Data from API
def fetch_data():
    url = f"{BASE_URL}?api_key={API_KEY}"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()

    print(f"Error {response.status_code}: {response.text}")
    return None

# Insert Competitors Data
def insert_competitors(data):
    if not data or "rankings" not in data:
        print("No competitor data available or endpoint issue.")
        return

    connection = get_db_connection()
    cursor = connection.cursor()
    competitors_inserted = set()

    for ranking in data["rankings"]:
        for competitor_data in ranking.get("competitor_rankings", []):
            competitor = competitor_data.get("competitor", {})
            competitor_id = competitor.get("id")

            if competitor_id and competitor_id not in competitors_inserted:
                cursor.execute("""
                    INSERT IGNORE INTO competitors 
                    (competitor_id, name, country, country_code, abbreviation)
                    VALUES (%s, %s, %s, %s, %s)
                """, (competitor_id, competitor.get("name", "Unknown"),
                      competitor.get("country", "Unknown"),
                      competitor.get("country_code", "N/A"),
                      competitor.get("abbreviation", "N/A")))
                competitors_inserted.add(competitor_id)

    connection.commit()
    cursor.close()
    connection.close()
    print("Competitors Data Inserted Successfully!")

# Insert Rankings Data
def insert_rankings(data):
    if not data or "rankings" not in data:
        print("No rankings data available or endpoint issue.")
        return

    connection = get_db_connection()
    cursor = connection.cursor()

    for ranking in data["rankings"]:
        for competitor_data in ranking.get("competitor_rankings", []):
            competitor = competitor_data.get("competitor", {})
            competitor_id = competitor.get("id")

            if competitor_id:
                cursor.execute("""
                    INSERT IGNORE INTO competitor_rankings 
                    (ranking_position, movement, points, competitions_played, competitor_id)
                    VALUES (%s, %s, %s, %s, %s)
                """, (competitor_data.get("rank", 0),
                      competitor_data.get("movement", 0),
                      competitor_data.get("points", 0),
                      competitor_data.get("competitions_played", 0),
                      competitor_id))

    connection.commit()
    cursor.close()
    connection.close()
    print("Competitor Rankings Data Inserted Successfully!")

# Main Execution
if __name__ == "__main__":
    create_tables()
    data = fetch_data()
    if data:
        insert_competitors(data)
        insert_rankings(data)


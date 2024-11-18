from bs4 import BeautifulSoup
import requests
from db.db_manager import connect_db, close_db
from fake_useragent import UserAgent


class Scraper:
    HEADERS = {'User-Agent': UserAgent().chrome}

    def __init__(self):
        self.conn = connect_db()

    def get_soup(self, url):
        """Fetches and parses HTML content from a URL."""
        try:
            response = requests.get(url, headers=self.HEADERS)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return None

    def save_to_db(self, query, data):
        """Executes an insert or update query with provided data."""
        cursor = self.conn.cursor()
        cursor.execute(query, data)
        self.conn.commit()

    def close_connection(self):
        """Closes the database connection."""
        close_db(self.conn)

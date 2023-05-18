import requests
from bs4 import BeautifulSoup

# Send a GET request to the webpage
url = "https://www.sharesansar.com/today-share-price"
response = requests.get(url)

# Parse the HTML content using BeautifulSoup
soup = BeautifulSoup(response.content, 'html.parser')

# Find the table containing the share price data
table = soup.find('table', {'id': 'headFixed'})

# Extract the table headers
headers = [header.text.strip() for header in table.find_all('th')]

# Extract the share price data rows
rows = table.find_all('tr')[1:]  # Exclude the header row

# Iterate over each row and extract the data
for row in rows:
    data = [cell.text.strip() for cell in row.find_all('td')]
    print(data)  # You can further process or store the data as per your requirement

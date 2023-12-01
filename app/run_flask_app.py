from flask import Flask, render_template
from pyhive import hive

app = Flask(__name__)


def fetch_data_from_hive():
    # Establish a connection to Hive
    conn = hive.connect(host='your_hive_host', port=10000, username='your_username')

    # Create a Hive cursor
    cursor = conn.cursor()

    # Execute the Hive query to fetch data from the table
    cursor.execute('SELECT * FROM music_and_genres LIMIT 10')

    # Fetch all rows from the result set
    data = cursor.fetchall()

    # Close the cursor and connection
    cursor.close()
    conn.close()

    return data


@app.route('/')
def show_table():
    # Fetch data from Hive
    data = fetch_data_from_hive()

    # Render the template with the fetched data
    return render_template('table.html', data=data)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

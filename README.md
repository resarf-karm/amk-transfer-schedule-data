# Data Import and Stored Procedure Execution

This project connects to MySQL and SQL Server databases, executes stored procedures, imports data into a SQL table, and handles database connections using SQLAlchemy.

## Prerequisites

- Python 3.x
- MySQL server
- SQL Server
- ODBC Driver 17 for SQL Server

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/resarf-karm/amk-transfer-schedule-data.git
    cd your-repo
    ```

2. Create a virtual environment and activate it:
    ```sh
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3. Install the required packages:
    ```sh
    pip install -r requirements.txt
    ```

4. Create a `.env` file in the root directory of the project and add your database credentials:
    ```properties
    DB_MI_USER=your_sqlserver_username
    DB_MI_PASSWORD=your_sqlserver_password
    DB_AMK_USER=your_mysql_username
    DB_AMK_PASSWORD=your_mysql_password
    ```

## Usage

1. Run the main script:
    ```sh
    python main.py
    ```

2. The script will:
    - Connect to the MySQL database and execute the `ap_scheduled_data` stored procedure.
    - Add an `import_date` column with the current date to the DataFrame.
    - Connect to the SQL Server database and import the DataFrame into the `stg_scheduled_data_import` table.
    - Execute the `ap_scheduled_data_import` stored procedure on the SQL Server database.

## Project Structure

- `main.py`: The main script that handles database connections, executes stored procedures, and imports data.
- `.env`: Environment file containing database credentials (not included in the repository).

## Dependencies

See `requirements.txt` for a list of dependencies.

## License

This project is licensed under the MIT License.
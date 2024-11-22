import os
import logging
import pyodbc
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from urllib import parse
from contextlib import contextmanager
from dotenv import load_dotenv

load_dotenv()

@contextmanager
def get_database_engine(db_name, db_host, db_type):
    """Context manager to handle database engine setup and teardown."""
    engine = None
    try:
        if db_type == 'mysql':
            user = os.getenv('DB_AMK_USER')
            password = os.getenv('DB_AMK_PASSWORD')
            database_url = f'mysql+pymysql://{user}:{parse.quote_plus(password)}@{db_host}/{db_name}'
        elif db_type == 'sqlserver':
            connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={db_host};"
                f"DATABASE={db_name};"
                f"UID={os.getenv('DB_MI_USER')};"
                f"PWD={os.getenv('DB_MI_PASSWORD')}"
            )
            database_url = f"mssql+pyodbc:///?odbc_connect={parse.quote_plus(connection_string)}"
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

        engine = create_engine(database_url)
        yield engine  # Return the engine and pause here
    except Exception as e:
        logging.error(f"Error setting up database connection for {db_name}: {e}")
        raise
    finally:
        if engine:
            engine.dispose()  # Dispose of the engine when done
            logging.info(f"Database engine for {db_name} closed.")

def execute_stored_procedure(engine, procedure_name, db_type, responce=False):
    """Execute a stored procedure and return the result as a DataFrame."""
    try:
        with engine.connect() as connection:
            if db_type == 'mysql':
                result = connection.execute(text(f"CALL {procedure_name}()"))
            elif db_type == 'sqlserver':
                connection = connection.execution_options(autocommit=True)
                result = connection.execute(text(f"EXEC {procedure_name}"))
            else:
                raise ValueError(f"Unsupported database type: {db_type}")
            
            if responce:
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return df
            else:
                connection.commit()
                return result
    except Exception as e:
        logging.error(f"Error executing stored procedure {procedure_name}: {e}")
        raise

def import_data_to_sql(df, table_name, engine, chunksize=5000):
    """Import a DataFrame into a SQL table."""
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False, chunksize=chunksize)
        logging.info(f"Data imported to table {table_name} successfully.")
    except Exception as e:
        logging.error(f"Error importing data to table {table_name}: {e}")
        raise

def main():
    try:
        with get_database_engine(db_name="arcus_internal", db_host='10.0.3.21', db_type='mysql') as engine:
            df = execute_stored_procedure(engine, procedure_name='ap_scheduled_data', db_type='mysql', responce=True)
    except Exception as e:
        logging.error(f"Error: {e}")

    if not df.empty:
        try:
            with get_database_engine("MI_DStore", db_host='vazmisql03.database.windows.net', db_type='sqlserver') as engine:
                import_data_to_sql(df, 'stg_scheduled_data_import', engine)
                execute_stored_procedure(engine, procedure_name='ap_scheduled_data_import', db_type='sqlserver', responce=False)
                print('completed')
        except Exception:
            return

if __name__ == "__main__":
    main()

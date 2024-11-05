import pyodbc
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_sql_connection():
    """
    Test połączenia z SQL Server używając Windows Authentication
    """
    try:
        # Lista dostępnych sterowników
        logger.info("Dostępne sterowniki ODBC:")
        for driver in pyodbc.drivers():
            logger.info(f"- {driver}")

        # String połączenia
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=serwer.domena.com;"
            "DATABASE=nazwa_bazy;"
            "Authentication=ActiveDirectoryIntegrated;"
            "TrustServerCertificate=yes;"
            "Encrypt=yes"
        )

        # Próba połączenia
        logger.info("Próba połączenia z bazą...")
        conn = pyodbc.connect(conn_str)
        
        # Test połączenia
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        logger.info(f"Połączono z SQL Server\nWersja: {version}")

        # Sprawdzenie użytkownika
        cursor.execute("SELECT SYSTEM_USER, USER_NAME()")
        system_user, database_user = cursor.fetchone()
        logger.info(f"Użytkownik systemowy: {system_user}")
        logger.info(f"Użytkownik bazy: {database_user}")

        cursor.close()
        conn.close()
        logger.info("Test zakończony pomyślnie")

    except pyodbc.Error as e:
        logger.error(f"Błąd ODBC: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Błąd ogólny: {str(e)}")
        raise

if __name__ == "__main__":
    test_sql_connection()
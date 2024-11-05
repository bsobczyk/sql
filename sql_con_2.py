from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import URL
import logging
import pyodbc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SQLServerADAuth:
    def __init__(self, server, database, username, password):
        """
        Inicjalizacja połączenia do SQL Server z uwierzytelnianiem AD
        
        Args:
            server (str): Nazwa serwera SQL
            database (str): Nazwa bazy danych
            username (str): Nazwa użytkownika (email lub UPN)
            password (str): Hasło użytkownika
        """
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.engine = None
        self.Session = None

    def create_connection_url(self):
        """
        Tworzy string połączenia dla SQL Server
        """
        params = {
            "DRIVER": "{ODBC Driver 18 for SQL Server}",
            "SERVER": self.server,
            "DATABASE": self.database,
            "UID": self.username,
            "PWD": self.password,
            "Authentication": "ActiveDirectoryPassword",
            "TrustServerCertificate": "yes",
            "Encrypt": "yes"
        }
        
        # Tworzenie stringa połączenia
        conn_str = ';'.join([f"{k}={v}" for k, v in params.items()])
        logger.info(f"String połączenia: {conn_str}")
        
        return conn_str

    def connect(self):
        """
        Nawiązuje połączenie z bazą danych
        """
        try:
            # Wyświetl dostępne sterowniki
            logger.info("Dostępne sterowniki ODBC:")
            for driver in pyodbc.drivers():
                logger.info(f"- {driver}")

            # Utwórz string połączenia
            conn_str = self.create_connection_url()
            
            # Próba bezpośredniego połączenia przez ODBC
            logger.info("Próba połączenia ODBC...")
            conn = pyodbc.connect(conn_str, timeout=30)
            
            # Test połączenia ODBC
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            logger.info(f"Połączono z SQL Server. Wersja: {version}")
            cursor.close()
            conn.close()
            
            # Jeśli ODBC działa, utwórz engine SQLAlchemy
            logger.info("Tworzenie engine SQLAlchemy...")
            self.engine = create_engine(
                f"mssql+pyodbc:///?odbc_connect={conn_str}",
                echo=False,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            # Utworzenie fabryki sesji
            self.Session = sessionmaker(bind=self.engine)
            
            return self.engine
            
        except Exception as e:
            logger.error(f"Błąd podczas łączenia z bazą: {str(e)}")
            raise

    def test_connection(self):
        """
        Test połączenia z bazą
        """
        try:
            with self.get_session() as session:
                # Test podstawowych informacji
                result = session.execute(text("""
                    SELECT 
                        @@VERSION as version,
                        CURRENT_USER as current_user,
                        DB_NAME() as database_name,
                        CONNECTIONPROPERTY('protocol_type') as protocol,
                        CONNECTIONPROPERTY('auth_scheme') as auth_scheme
                """))
                row = result.fetchone()
                
                logger.info("Informacje o połączeniu:")
                logger.info(f"Wersja SQL: {row.version.split('\n')[0]}")
                logger.info(f"Użytkownik: {row.current_user}")
                logger.info(f"Baza: {row.database_name}")
                logger.info(f"Protokół: {row.protocol}")
                logger.info(f"Schemat auth: {row.auth_scheme}")
                
        except Exception as e:
            logger.error(f"Błąd podczas testu połączenia: {str(e)}")
            raise

    def get_session(self):
        """
        Zwraca nową sesję SQLAlchemy
        """
        if not self.Session:
            raise Exception("Połączenie nie zostało zainicjalizowane!")
        return self.Session()

# Przykład użycia
if __name__ == "__main__":
    try:
        # Konfiguracja połączenia
        config = {
            "server": "serwer.database.windows.net",  # Pełna nazwa serwera
            "database": "nazwa_bazy",
            "username": "user@domena.com",  # Użyj pełnego adresu email lub UPN
            "password": "hasło"
        }
        
        # Utworzenie instancji i połączenie
        db = SQLServerADAuth(**config)
        db.connect()
        db.test_connection()
        
    except Exception as e:
        logger.error(f"Błąd główny: {str(e)}")
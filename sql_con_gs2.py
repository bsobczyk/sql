from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import URL
import logging
import subprocess
from pathlib import Path
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SQLServerWindowsAuth:
    def __init__(self, server, database, domain, username, password):
        """
        Inicjalizacja połączenia do SQL Server z uwierzytelnianiem Windows
        
        Args:
            server (str): Nazwa serwera SQL
            database (str): Nazwa bazy danych
            domain (str): Nazwa domeny (np. FIRMA.COM)
            username (str): Nazwa użytkownika domenowego
            password (str): Hasło użytkownika
        """
        self.server = server
        self.database = database
        self.domain = domain.upper()
        self.username = username
        self.password = password
        self.engine = None
        self.Session = None

    def setup_kerberos(self):
        """
        Konfiguruje Kerberos i uzyskuje bilet
        """
        try:
            # Usuń stare bilety
            subprocess.run(['kdestroy', '-A'], stderr=subprocess.DEVNULL)
            
            # Przygotuj principal name
            principal = f"{self.username}@{self.domain}"
            
            # Uzyskaj bilet Kerberos
            kinit = subprocess.Popen(
                ['kinit', principal],
                stdin=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            kinit.communicate(input=self.password.encode())
            
            if kinit.returncode != 0:
                raise Exception("Błąd podczas uzyskiwania biletu Kerberos")
            
            # Sprawdź bilet
            klist = subprocess.run(['klist'], capture_output=True, text=True)
            logger.info(f"Status biletów Kerberos:\n{klist.stdout}")
            
        except Exception as e:
            logger.error(f"Błąd konfiguracji Kerberos: {str(e)}")
            raise

    def create_connection_string(self):
        """
        Tworzy string połączenia dla SQL Server
        """
        return URL.create(
            "mssql+pyodbc",
            query={
                "driver": "ODBC Driver 18 for SQL Server",
                "server": self.server,
                "database": self.database,
                "Authentication": "ActiveDirectoryPassword",
                "UID": f"{self.username}@{self.domain}",
                "PWD": self.password,
                "TrustServerCertificate": "yes",
                "Encrypt": "yes"
            }
        )

    def connect(self):
        """
        Nawiązuje połączenie z bazą danych
        """
        try:
            # Konfiguracja połączenia
            conn_url = self.create_connection_string()
            
            # Utworzenie silnika SQLAlchemy
            self.engine = create_engine(
                conn_url,
                echo=False,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            # Utworzenie fabryki sesji
            self.Session = sessionmaker(bind=self.engine)
            
            # Test połączenia
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT SYSTEM_USER, USER_NAME()"))
                system_user, database_user = result.fetchone()
                logger.info(f"Połączono jako użytkownik systemowy: {system_user}")
                logger.info(f"Użytkownik bazy danych: {database_user}")
            
            return self.engine
            
        except Exception as e:
            logger.error(f"Błąd podczas łączenia z bazą: {str(e)}")
            raise

    def get_session(self):
        """
        Zwraca nową sesję SQLAlchemy
        """
        if not self.Session:
            raise Exception("Połączenie nie zostało zainicjalizowane!")
        return self.Session()

    def test_connection(self):
        """
        Wykonuje test połączenia
        """
        try:
            with self.get_session() as session:
                # Test wersji SQL Server
                result = session.execute(text("SELECT @@VERSION"))
                version = result.scalar()
                logger.info(f"Wersja SQL Server: {version}")

                # Test uprawnień
                result = session.execute(text("""
                    SELECT 
                        dp.name as principal_name,
                        dp.type_desc as principal_type,
                        o.name as object_name,
                        p.permission_name
                    FROM sys.database_permissions p
                    JOIN sys.database_principals dp ON p.grantee_principal_id = dp.principal_id
                    LEFT JOIN sys.objects o ON p.major_id = o.object_id
                    WHERE dp.name = SYSTEM_USER
                """))
                
                logger.info("Uprawnienia użytkownika:")
                for row in result:
                    logger.info(f"- {row.permission_name} na {row.object_name or 'DATABASE'}")

        except Exception as e:
            logger.error(f"Błąd podczas testu połączenia: {str(e)}")
            raise

# Przykład użycia
if __name__ == "__main__":
    # Konfiguracja połączenia
    config = {
        "server": "sql-server.firma.com",  # Zmień na swój serwer
        "database": "nazwa_bazy",          # Zmień na swoją bazę
        "domain": "FIRMA.COM",            # Zmień na swoją domenę
        "username": "uzytkownik",         # Zmień na swojego użytkownika
        "password": "haslo"               # Zmień na swoje hasło
    }
    
    try:
        # Utworzenie instancji i połączenie
        db = SQLServerWindowsAuth(**config)
        db.connect()
        
        # Test połączenia
        db.test_connection()
        
        # Przykład użycia sesji
        with db.get_session() as session:
            result = session.execute(text("SELECT DB_NAME() as current_db"))
            current_db = result.scalar()
            print(f"Aktualna baza danych: {current_db}")
            
    except Exception as e:
        print(f"Wystąpił błąd: {str(e)}")
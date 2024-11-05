from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import URL
import gssapi
import logging
import os
import subprocess
from pathlib import Path

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
        
    def setup_krb5_config(self):
        """
        Tworzy tymczasowy plik konfiguracyjny Kerberos
        """
        krb5_conf = f"""[libdefaults]
default_realm = {self.domain}
dns_lookup_realm = false
dns_lookup_kdc = true
ticket_lifetime = 24h
forwardable = true
proxiable = true

[realms]
{self.domain} = {{
    kdc = {self.domain.lower()}
    admin_server = {self.domain.lower()}
}}

[domain_realm]
.{self.domain.lower()} = {self.domain}
{self.domain.lower()} = {self.domain}
"""
        # Zapisz do pliku w katalogu użytkownika
        krb5_path = Path.home() / '.krb5.conf'
        krb5_path.write_text(krb5_conf)
        os.environ['KRB5_CONFIG'] = str(krb5_path)
        return krb5_path

    def get_ticket(self):
        """
        Uzyskuje bilet Kerberos używając loginu i hasła
        """
        try:
            # Usuń stare bilety
            subprocess.run(['kdestroy'], stderr=subprocess.DEVNULL)
            
            # Przygotuj principal
            principal = f"{self.username}@{self.domain}"
            
            # Utwórz credential cache
            ccache = "FILE:/tmp/krb5cc_{}".format(os.getuid())
            os.environ['KRB5CCNAME'] = ccache
            
            # Uzyskaj bilet używając loginu i hasła
            name = gssapi.Name(principal, name_type=gssapi.NameType.user)
            store = {'ccache': ccache, 'client_name': name}
            
            flags = (
                gssapi.RequirementFlag.delegate_to_peer |
                gssapi.RequirementFlag.mutual_authentication |
                gssapi.RequirementFlag.out_of_sequence_detection
            )
            
            gssapi.Credentials(usage='initiate', name=name, password=self.password,
                             store=store, flags=flags)
            
            logger.info(f"Uzyskano bilet Kerberos dla {principal}")
            return True
            
        except gssapi.exceptions.GSSError as e:
            logger.error(f"Błąd podczas uzyskiwania biletu Kerberos: {str(e)}")
            raise
    
    def create_connection_url(self):
        """
        Tworzy URL połączenia dla SQLAlchemy
        """
        connection_url = URL.create(
            "mssql+pyodbc",
            query={
                "driver": "ODBC Driver 18 for SQL Server",
                "TrustServerCertificate": "yes",
                "Authentication": "ActiveDirectoryIntegrated",
                "Encrypt": "yes"
            },
            host=self.server,
            database=self.database,
        )
        return connection_url

    def connect(self):
        """
        Nawiązuje połączenie z bazą danych
        """
        try:
            # Konfiguracja Kerberos
            krb5_path = self.setup_krb5_config()
            self.get_ticket()
            
            # Utworzenie silnika SQLAlchemy
            self.engine = create_engine(
                self.create_connection_url(),
                echo=False,
                pool_pre_ping=True
            )
            
            # Utworzenie fabryki sesji
            self.Session = sessionmaker(bind=self.engine)
            
            # Test połączenia
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT SYSTEM_USER"))
                logger.info(f"Połączono jako: {result.scalar()}")
            
            return self.engine
        
        except Exception as e:
            logger.error(f"Błąd podczas łączenia z bazą: {str(e)}")
            raise
        finally:
            # Usuń tymczasowy plik konfiguracyjny
            if krb5_path.exists():
                krb5_path.unlink()
    
    def get_session(self):
        """
        Zwraca nową sesję SQLAlchemy
        """
        if not self.Session:
            raise Exception("Połączenie nie zostało zainicjalizowane!")
        return self.Session()

# Przykład użycia
def example_usage():
    # Konfiguracja połączenia
    config = {
        "server": "sql-server.firma.com",
        "database": "nazwa_bazy",
        "domain": "FIRMA.COM",
        "username": "user",
        "password": "haslo"
    }
    
    # Utworzenie instancji
    db = SQLServerWindowsAuth(**config)
    
    try:
        # Nawiązanie połączenia
        db.connect()
        
        # Przykład użycia
        with db.get_session() as session:
            result = session.execute(text("SELECT @@VERSION"))
            version = result.scalar()
            print(f"Wersja SQL Server: {version}")
            
    except Exception as e:
        logger.error(f"Wystąpił błąd: {str(e)}")
        raise

if __name__ == "__main__":
    example_usage()
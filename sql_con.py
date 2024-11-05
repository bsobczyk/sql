from sqlalchemy import create_engine, Column, Integer, String, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.engine import URL
from urllib.parse import quote_plus
import logging

# Konfiguracja logowania
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Utworzenie bazowej klasy dla modeli
Base = declarative_base()

# Przykładowy model - możesz dostosować do swoich potrzeb
class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    username = Column(String(100))
    email = Column(String(100))

class SQLAlchemyADConnection:
    def __init__(self, server, database, username, password, ad_domain=None):
        """
        Inicjalizacja połączenia SQLAlchemy z uwierzytelnianiem AD
        
        Args:
            server (str): Nazwa serwera SQL
            database (str): Nazwa bazy danych
            username (str): Nazwa użytkownika AD
            password (str): Hasło użytkownika
            ad_domain (str, optional): Domena AD
        """
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.ad_domain = ad_domain
        self.engine = None
        self.Session = None

    def create_connection_url(self):
        """
        Tworzy URL połączenia dla SQLAlchemy
        """
        # Dodaj domenę do nazwy użytkownika jeśli została podana
        auth_username = (
            f"{self.ad_domain}\\{self.username}" 
            if self.ad_domain 
            else self.username
        )
        
        # Tworzenie connection URL używając klasy URL z SQLAlchemy
        connection_url = URL.create(
            "mssql+pyodbc",
            query={
                "driver": "ODBC Driver 17 for SQL Server",
                "TrustServerCertificate": "yes",
                "Encrypt": "yes",
            },
            username=auth_username,
            password=self.password,
            host=self.server,
            database=self.database,
        )
        
        return connection_url

    def connect(self):
        """
        Tworzy połączenie i sesję SQLAlchemy
        """
        try:
            # Utworzenie silnika SQLAlchemy
            self.engine = create_engine(
                self.create_connection_url(),
                echo=False,  # Ustaw na True dla debugowania SQL
                pool_pre_ping=True,  # Sprawdza połączenie przed użyciem
                pool_recycle=3600,  # Odśwież połączenia po godzinie
            )
            
            # Utworzenie fabryki sesji
            self.Session = sessionmaker(bind=self.engine)
            
            # Test połączenia
            with self.engine.connect() as connection:
                result = connection.execute(text("SELECT @@VERSION"))
                version = result.scalar()
                logger.info(f"Połączono z SQL Server. Wersja: {version}")
                
            return self.engine
            
        except Exception as e:
            logger.error(f"Błąd podczas łączenia z bazą: {str(e)}")
            raise

    def create_tables(self):
        """
        Tworzy wszystkie zdefiniowane tabele w bazie
        """
        try:
            Base.metadata.create_all(self.engine)
            logger.info("Tabele zostały utworzone pomyślnie")
        except Exception as e:
            logger.error(f"Błąd podczas tworzenia tabel: {str(e)}")
            raise

    def test_connection(self):
        """
        Testuje połączenie wykonując przykładowe zapytania
        """
        try:
            with self.Session() as session:
                # Sprawdź tożsamość użytkownika
                result = session.execute(text("SELECT SYSTEM_USER, CURRENT_USER"))
                system_user, database_user = result.first()
                logger.info(f"Zalogowany użytkownik systemowy: {system_user}")
                logger.info(f"Zalogowany użytkownik bazy: {database_user}")
                
                # Sprawdź uprawnienia
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
            logger.error(f"Błąd podczas testowania połączenia: {str(e)}")
            raise

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
        "server": "twoj-serwer.database.windows.net",
        "database": "twoja-baza",
        "username": "twoj_uzytkownik",
        "password": "twoje_haslo",
        "ad_domain": "TWOJA-DOMENA"  # opcjonalne
    }
    
    # Utworzenie instancji klasy połączenia
    db = SQLAlchemyADConnection(**config)
    
    try:
        # Nawiązanie połączenia
        db.connect()
        
        # Test połączenia
        db.test_connection()
        
        # Utworzenie tabel (jeśli potrzebne)
        db.create_tables()
        
        # Przykład użycia sesji
        with db.get_session() as session:
            # Dodanie nowego użytkownika
            new_user = User(username="jan.kowalski", email="jan.kowalski@firma.com")
            session.add(new_user)
            
            # Zapytanie o użytkowników
            users = session.query(User).all()
            for user in users:
                print(f"Użytkownik: {user.username}, Email: {user.email}")
                
            session.commit()
            
    except Exception as e:
        logger.error(f"Wystąpił błąd: {str(e)}")
        raise

if __name__ == "__main__":
    example_usage()
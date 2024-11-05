import pyodbc
from urllib.parse import quote_plus

class ADSQLConnection:
    def __init__(self, server, database, username, password, ad_domain=None):
        """
        Inicjalizuje połączenie SQL z uwierzytelnianiem AD
        
        Args:
            server (str): Nazwa serwera SQL
            database (str): Nazwa bazy danych
            username (str): Nazwa użytkownika AD
            password (str): Hasło użytkownika
            ad_domain (str): Domena AD (opcjonalne)
        """
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.ad_domain = ad_domain
        self.conn = None
    
    def connect_with_ad_user(self):
        """
        Tworzy połączenie używając poświadczeń AD
        """
        try:
            # Jeśli podano domenę, dodaj ją do nazwy użytkownika
            auth_username = (
                f"{self.ad_domain}\\{self.username}" 
                if self.ad_domain 
                else self.username
            )
            
            # Konfiguracja stringa połączenia
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={auth_username};"
                f"PWD={self.password};"
                "Encrypt=yes;"
                "TrustServerCertificate=yes;"
            )
            
            # Utworzenie połączenia
            self.conn = pyodbc.connect(conn_str)
            print("Połączono pomyślnie z bazą danych!")
            return self.conn
            
        except pyodbc.Error as e:
            print(f"Błąd podczas łączenia z bazą: {str(e)}")
            raise

    def test_connection(self):
        """
        Testuje połączenie wykonując proste zapytanie
        """
        if not self.conn:
            raise Exception("Brak aktywnego połączenia!")
            
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT SYSTEM_USER, CURRENT_USER")
            row = cursor.fetchone()
            print(f"Zalogowany użytkownik systemowy: {row[0]}")
            print(f"Zalogowany użytkownik bazy: {row[1]}")
            
            # Sprawdź uprawnienia użytkownika
            cursor.execute("""
                SELECT 
                    dp.name as principal_name,
                    dp.type_desc as principal_type,
                    o.name as object_name,
                    p.permission_name
                FROM sys.database_permissions p
                JOIN sys.database_principals dp ON p.grantee_principal_id = dp.principal_id
                LEFT JOIN sys.objects o ON p.major_id = o.object_id
                WHERE dp.name = SYSTEM_USER
            """)
            
            print("\nUprawnienia użytkownika:")
            for row in cursor.fetchall():
                print(f"- {row.permission_name} na {row.object_name or 'DATABASE'}")
                
        except pyodbc.Error as e:
            print(f"Błąd podczas wykonywania zapytania: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()
    
    def execute_query(self, query, params=None):
        """
        Wykonuje zapytanie SQL
        
        Args:
            query (str): Zapytanie SQL
            params (tuple): Parametry zapytania (opcjonalne)
        """
        if not self.conn:
            raise Exception("Brak aktywnego połączenia!")
            
        try:
            cursor = self.conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor
        except pyodbc.Error as e:
            print(f"Błąd podczas wykonywania zapytania: {str(e)}")
            raise
    
    def close(self):
        """
        Zamyka połączenie z bazą
        """
        if self.conn:
            self.conn.close()
            print("Połączenie zostało zamknięte.")

# Przykład użycia
if __name__ == "__main__":
    # Konfiguracja połączenia
    SERVER = "twoj-serwer.database.windows.net"
    DATABASE = "twoja-baza"
    USERNAME = "twoj_uzytkownik"
    PASSWORD = "twoje_haslo"
    AD_DOMAIN = "TWOJA-DOMENA"  # opcjonalne
    
    # Utworzenie instancji klasy i połączenie
    sql_conn = ADSQLConnection(
        server=SERVER,
        database=DATABASE,
        username=USERNAME,
        password=PASSWORD,
        ad_domain=AD_DOMAIN
    )
    
    # Nawiązanie połączenia i test
    sql_conn.connect_with_ad_user()
    sql_conn.test_connection()
    
    # Przykład wykonania zapytania
    cursor = sql_conn.execute_query("SELECT TOP 5 * FROM twoja_tabela")
    for row in cursor.fetchall():
        print(row)
    
    # Zamknięcie połączenia
    sql_conn.close()
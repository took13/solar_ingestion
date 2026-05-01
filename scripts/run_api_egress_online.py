import pyodbc

from src.egress.enserve_job import EnserveEgressJob


def main():
    connection_string = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=SOLAR-DB\\SOLARSQL;"
        "DATABASE=SolarDataDB;"
        "UID=sa;"
        "PWD=p@ssw0rd;"
        "TrustServerCertificate=yes;"
    )

    conn = pyodbc.connect(connection_string)
    try:
        job = EnserveEgressJob(conn)
        job.run()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
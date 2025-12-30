from db import execute_sql, missing_credentials

def main():
    missing = missing_credentials()
    if missing:
        print("Databricks credentials are missing.")
        print("Set these environment variables (or use Streamlit secrets / a .env file):")
        for k in missing:
            print(f"- {k}")
        return

    df = execute_sql("SELECT 1 as ok LIMIT 1")
    print(df)

if __name__ == '__main__':
    main()

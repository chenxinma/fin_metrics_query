from pathlib import Path
import kuzu

def main():
    # Create an empty on-disk database and connect to it
    db = kuzu.Database('./kuzudb')
    conn = kuzu.Connection(db)

    directory = Path('./data')
    print("Init schema")
    with open(directory / 'schema.sql', 'r', encoding='utf-8') as f:
        ddl = f.read()
        conn.execute(ddl)
    
    print("Init data")
    with open(directory / 'data.sql', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        ddl = ''
        for line in lines:
            ddl += line.strip()
            if ddl.endswith(';'):
                conn.execute(ddl)
                ddl = ''
    conn.close()


if __name__ == "__main__":
    main()
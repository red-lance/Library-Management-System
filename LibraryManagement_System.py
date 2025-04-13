import mysql.connector
import pandas as pd

DB_NAME = "LibraryDB"
TABLE_NAME = "Books"


def connect_db():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="rishi2005"
    )
    cursor = conn.cursor()
    return conn, cursor


def setup_database(cursor):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    cursor.execute(f"USE {DB_NAME}")

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id VARCHAR(10) PRIMARY KEY,
            title VARCHAR(255),
            author VARCHAR(255),
            category VARCHAR(100),
            cabinet VARCHAR(50),
            rack VARCHAR(50),
            row_num VARCHAR(50),
            signal_strength VARCHAR(50),
            timestamp VARCHAR(255),
            status VARCHAR(255)
        )
    """)

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS DeletedBooks (
            id VARCHAR(10),
            title VARCHAR(255),
            author VARCHAR(255),
            category VARCHAR(100),
            cabinet VARCHAR(50),
            rack VARCHAR(50),
            row_num VARCHAR(50),
            signal_strength VARCHAR(50),
            timestamp VARCHAR(255),
            status VARCHAR(255),
            deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute(f"DROP TRIGGER IF EXISTS before_book_delete")
    cursor.execute(f"""
        CREATE TRIGGER before_book_delete
        BEFORE DELETE ON {TABLE_NAME}
        FOR EACH ROW
        INSERT INTO DeletedBooks
        VALUES (
            OLD.id,
            OLD.title,
            OLD.author,
            OLD.category,
            OLD.cabinet,
            OLD.rack,
            OLD.row_num,
            OLD.signal_strength,
            OLD.timestamp,
            OLD.status,
            NOW()
        )
    """)


def import_csv(cursor, conn, file_path):
    df = pd.read_csv(file_path)
    for _, row in df.iterrows():
        cursor.execute(f"""
            INSERT IGNORE INTO {TABLE_NAME}
            (id, title, author, category, cabinet, rack, row_num, signal_strength, timestamp, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['Book_ID'],
            row['Title'],
            row['Author'],
            row['Category'],
            row['Cabinet'],
            row['Rack'],
            row['Row'],
            row['Signal_Strength'],
            row['Timestamp'],
            row['Status']
        ))
    conn.commit()
    print("CSV data imported successfully.\n")


def display_books(cursor):
    cursor.execute(f"SELECT * FROM {TABLE_NAME}")
    rows = cursor.fetchall()
    print("\nBook List:")
    for row in rows:
        print(row)


def show_structure(cursor):
    cursor.execute(f"DESCRIBE {TABLE_NAME}")
    structure = cursor.fetchall()
    print("\nTable Structure:")
    for column in structure:
        print(column)


def show_deleted_books(cursor):
    cursor.execute("SELECT * FROM DeletedBooks")
    rows = cursor.fetchall()
    print("\nDeleted Books Log:")
    for row in rows:
        print(row)


def add_book(cursor, conn):
    data = input("Enter book details (id, title, author, category, cabinet, rack, row, signal_strength, timestamp, status):\n").split(",")
    cursor.execute(f"""
        INSERT INTO {TABLE_NAME}
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, tuple(data))
    conn.commit()
    print("Book added successfully.")


def update_book(cursor, conn):
    book_id = input("Enter the Book ID to update: ")
    column = input("Enter column to update (e.g., title, author, status): ")
    value = input(f"Enter new value for {column}: ")
    cursor.execute(f"UPDATE {TABLE_NAME} SET {column} = %s WHERE id = %s", (value, book_id))
    conn.commit()
    print("Book updated.")


def delete_book(cursor, conn):
    book_id = input("Enter the Book ID to delete: ")
    cursor.execute(f"DELETE FROM {TABLE_NAME} WHERE id = %s", (book_id,))
    conn.commit()
    print("Book deleted.")


def main_menu():
    conn, cursor = connect_db()
    setup_database(cursor)
    cursor.execute(f"USE {DB_NAME}")
    import_csv(cursor, conn, "library_dataset_random.csv")

    while True:
        print("\n===== Library Menu =====")
        print("1. Display all books")
        print("2. Add a book")
        print("3. Update a book")
        print("4. Delete a book")
        print("5. Show table structure")
        print("6. Show deleted books log")
        print("7. Exit")

        choice = input("Choose an option: ")

        if choice == "1":
            display_books(cursor)
        elif choice == "2":
            add_book(cursor, conn)
        elif choice == "3":
            update_book(cursor, conn)
        elif choice == "4":
            delete_book(cursor, conn)
        elif choice == "5":
            show_structure(cursor)
        elif choice == "6":
            show_deleted_books(cursor)
        elif choice == "7":
            break
        else:
            print("Invalid option. Try again.")

    cursor.close()
    conn.close()
    print("Goodbye!")


if __name__ == "__main__":
    main_menu()

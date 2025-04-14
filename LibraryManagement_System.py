import mysql.connector
import pandas as pd

DB_NAME = "LibraryDB"
TABLE_NAME = "Books"

def connect_db():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="yourpassword"
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
            timestamp VARCHAR(255),
            status VARCHAR(50)
        )
    """)

    # Create log_table
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS log_table (
            log_id INT AUTO_INCREMENT PRIMARY KEY,
            book_id VARCHAR(10),
            old_status VARCHAR(255),
            new_status VARCHAR(255),
            action_type VARCHAR(10),
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create borrowers table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Borrowers (
        borrow_id INT AUTO_INCREMENT PRIMARY KEY,
        book_id VARCHAR(10),
        borrower_name VARCHAR(100),
        borrow_date DATETIME DEFAULT CURRENT_TIMESTAMP,
        return_date DATETIME,
        FOREIGN KEY (book_id) REFERENCES Books(id)
        )
    """)

    # Drop and create AFTER UPDATE trigger
    cursor.execute("DROP TRIGGER IF EXISTS after_book_update")
    cursor.execute(f"""
        CREATE TRIGGER after_book_update
        AFTER UPDATE ON {TABLE_NAME}
        FOR EACH ROW
        BEGIN
            IF OLD.status != NEW.status THEN
                INSERT INTO log_table (book_id, old_status, new_status, action_type)
                VALUES (OLD.id, OLD.status, NEW.status, 'UPDATE');
            END IF;
        END;
    """)

    # Drop and create AFTER DELETE trigger
    cursor.execute("DROP TRIGGER IF EXISTS after_book_delete")
    cursor.execute(f"""
        CREATE TRIGGER after_book_delete
        AFTER DELETE ON {TABLE_NAME}
        FOR EACH ROW
        BEGIN
            INSERT INTO log_table (book_id, old_status, new_status, action_type)
            VALUES (OLD.id, OLD.status, NULL, 'DELETE');
        END;
    """)

def import_csv(cursor, conn, file_path):
    df = pd.read_csv(file_path)
    for _, row in df.iterrows():
        cursor.execute(f"""
            INSERT IGNORE INTO {TABLE_NAME}
            (id, title, author, category, cabinet, rack, row_num, timestamp, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['Book_ID'],
            row['Title'],
            row['Author'],
            row['Category'],
            row['Cabinet'],
            row['Rack'],
            row['Row'],
            row['Timestamp'],
            row['Status'][:50]
        ))
    conn.commit()
    print("CSV data imported successfully.")

def display_books(cursor):
    cursor.execute(f"SELECT * FROM {TABLE_NAME}")
    rows = cursor.fetchall()
    for row in rows:
        print(row)

def add_book(cursor, conn):
    data = input("Enter book details (id, title, author, category, cabinet, rack, row, timestamp, status):\n").split(",")
    data[-1] = data[-1][:50]  # Trim status
    cursor.execute(f"""
        INSERT INTO {TABLE_NAME}
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, tuple(data))
    conn.commit()
    print("Book added successfully.")

def update_book(cursor, conn):
    book_id = input("Enter the Book ID to update: ")
    cursor.execute(f"SELECT * FROM {TABLE_NAME} WHERE id = %s", (book_id,))
    if cursor.fetchone() is None:
        print("No book found with the given ID.")
        return
    column = input("Enter column to update (e.g., title, author, status): ")
    value = input(f"Enter new value for {column}: ")
    if column == "status":
        value = value[:50]
    cursor.execute(f"UPDATE {TABLE_NAME} SET {column} = %s WHERE id = %s", (value, book_id))
    conn.commit()
    print("Book updated.")

def delete_book(cursor, conn):
    book_id = input("Enter the Book ID to delete: ")
    cursor.execute(f"DELETE FROM {TABLE_NAME} WHERE id = %s", (book_id,))
    if cursor.rowcount == 0:
        print("No book found with the given ID.")
    else:
        conn.commit()
        print("Book deleted.")

def borrow_book(cursor, conn):
    book_id = input("Enter the Book ID to borrow: ").strip().upper()
    cursor.execute(f"SELECT status FROM {TABLE_NAME} WHERE id = %s", (book_id,))
    result = cursor.fetchone()
    
    if not result:
        print("Book not found.")
    elif result[0].lower() == "checked out":
        print("Book already checked out.")
    else:
        borrower_name = input("Enter your name: ").strip()
        cursor.execute(f"UPDATE {TABLE_NAME} SET status = 'Checked Out' WHERE id = %s", (book_id,))
        cursor.execute("""
            INSERT INTO Borrowers (book_id, borrower_name)
            VALUES (%s, %s)
        """, (book_id, borrower_name))
        conn.commit()
        print("Book borrowed successfully.")


def return_book(cursor, conn):
    book_id = input("Enter the Book ID to return: ").strip().upper()
    cursor.execute(f"SELECT status FROM {TABLE_NAME} WHERE id = %s", (book_id,))
    result = cursor.fetchone()

    if not result:
        print("Book not found.")
    elif result[0].lower() == "present":
        print("Book is already marked as present.")
    else:
        cursor.execute(f"UPDATE {TABLE_NAME} SET status = 'Present' WHERE id = %s", (book_id,))
        cursor.execute("""
            UPDATE Borrowers
            SET return_date = CURRENT_TIMESTAMP
            WHERE book_id = %s AND return_date IS NULL
        """, (book_id,))
        conn.commit()
        print("Book returned successfully.")


def show_structure(cursor):
    cursor.execute(f"DESCRIBE {TABLE_NAME}")
    structure = cursor.fetchall()
    print("\nTable Structure:")
    for column in structure:
        print(column)


def show_deleted_books(cursor):
    cursor.execute("SELECT * FROM log_table WHERE action_type = 'DELETE'")
    rows = cursor.fetchall()
    if not rows:
        print("No deleted books found.")
    else:
        print("\nDeleted Books:")
        for row in rows:
            print(row)


def show_borrowed_books(cursor):
    cursor.execute("""
        SELECT b.id, b.title, br.borrower_name, br.borrow_date
        FROM Books b
        JOIN Borrowers br ON b.id = br.book_id
        WHERE br.return_date IS NULL
    """)
    rows = cursor.fetchall()
    print("\nCurrently Borrowed Books:")
    for row in rows:
        print(f"ID: {row[0]}, Title: {row[1]}, Borrower: {row[2]}, Date: {row[3]}")



def main_menu():
    conn, cursor = connect_db()
    setup_database(cursor)
    cursor.execute(f"USE {DB_NAME}")
    import_csv(cursor, conn, "library_dataset_random.csv")

    while True:
        print("\nLibrary Menu")
        print("1. Display all books")
        print("2. Add a book")
        print("3. Update a book")
        print("4. Delete a book")
        print("5. Show table structure")
        print("6. Borrow a book")
        print("7. Return a book")
        print("8. Show deleted books")
        print("9. Show borrowed books")
        print("10. Exit")

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
            borrow_book(cursor, conn)
        elif choice == "7":
            return_book(cursor, conn)
        elif choice == "8":
            show_deleted_books(cursor)
        elif choice == "9":
            show_borrowed_books(cursor)
        elif choice == "10":
            break
        else:
            print("Invalid option. Try again.")

    cursor.close()
    conn.close()
    print("Exiting...")


if __name__ == "__main__":
    main_menu()
